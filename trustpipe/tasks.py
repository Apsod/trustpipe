import logging
import json
import secrets
import os
import pathlib
import tempfile

import luigi
import docker
import git

from trustpipe.util import meta_storage, data_storage

MSTORE = meta_storage()
DSTORE = data_storage()

logger = logging.getLogger('luigi-interface')

def clone_subpath(url, subpath, dst, branch=None):
    OPTIONS = ['-n', '--depth=1', '--no-checkout', '--filter=tree:0']
    if branch:
        OPTIONS += [f'--branch={branch}']

    repo = git.Repo.clone_from(
            url, 
            multi_options=OPTIONS,
            to_path=dst)
    repo.git.sparse_checkout('init')
    repo.git.sparse_checkout(['set', '--no-cone', subpath])
    repo.git.checkout()
    return repo.rev_parse('HEAD').hexsha

###
#
# PULLING STUFF FROM GITHUB
#
###

class PullTask(luigi.Task):
    location = luigi.Parameter()

    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter(default=None)

    prefix = luigi.Parameter(default='git@github.com:', significant=False)

    def output(self):
        return luigi.LocalTarget(self.location)

    def git_url(self):
        assert self.repo.endswith('.git'), 'repo must end with .git'
        assert not self.subpath.startswith('/'), 'subpaths must be relative'

        return f'{self.prefix}{self.repo}'

    
    def run(self):
        out = self.output()
        out.makedirs()

        out_path = pathlib.Path(out.path)
        rng = secrets.token_hex()

        with tempfile.TemporaryDirectory(prefix=out_path.name, suffix=rng, dir=out_path.parent) as tmpdir:
            tmp = pathlib.Path(tmpdir)
            tmpout = tmp/'out'
            sha = clone_subpath(self.git_url(), self.subpath, tmp/rng, branch=self.branch)
            os.mkdir(tmpout)
            os.rename(tmp/rng/self.subpath, tmpout/'repo')
            with open(tmpout/'info.json', 'wt') as handle:
                handle.write(json.dumps(dict(
                    hash=sha,
                    repo=self.repo,
                    path=self.subpath,
                    branch=self.branch,
                    )))
            os.rename(tmpout, out_path)

def build_pulled(trg, name, client, logger):
    root = pathlib.Path(trg.path)
    logger.info(f'building image @ {root}/repo')
    try:
        img, logs = client.images.build(path=str(root / 'repo'), tag=name)
        for log in logs:
            logger.info(json.dumps(log))
    except docker.errors.APIError as e:
        logger.warning('docker API error during build: ' + e.explanation)
        raise e
    except docker.errors.BuildError as e:
        logger.warning('docker build error during build: ' + e.explanation)
        raise e
    info = json.loads(open(root / 'info.json').read())
    info['image_id'] = img.id
    return img, info

###
#
# RUNNING PULLED INGEST DOCKERS
#
###

class IngestTask(luigi.Task):
    name = luigi.Parameter()

    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter(default=None)

    prefix = luigi.Parameter(default='git@github.com:', significant=False)
    
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the IngestTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        self.__logger = logger
        self._client = docker.client.from_env()
        
    def output(self):
        return MSTORE.get_target(self.name, 'ingest', 'data')

    def requires(self):
        my_dir = pathlib.Path(self.output().path).parent
        repo_path = pathlib.Path(self.subpath) / 'ingest'

        return PullTask(
                str(my_dir / 'pull'),
                self.repo,
                str(repo_path),
                self.branch,
                self.prefix)
    
    def run(self):
        img, info = build_pulled(self.input(), self.name, self._client, self.__logger)

        host_path = DSTORE.get_path(self.name, 'ingest')

        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)

        container = self._client.containers.run(
            img,
            volumes=[f'{host_path}:/data'],  # TODO: should we not depend on /data being where the container puts data?
            detach=True
        )
        
        for log in container.attach(stream=True):
            self.__logger.info(log.decode('utf-8'))
        
        with self.output().temporary_path() as tmpdir:
            root = pathlib.Path(tmpdir)
            root.mkdir(parents=True)

            os.symlink(host_path, root / 'data', target_is_directory=True)
            with open(root / 'info.json', 'wt') as out:
                out.write(json.dumps(info))

###
#
# RUNNING PULLED PROCESS DOCKERS
#
###

class ProcessTask(luigi.Task):
    name = luigi.Parameter()
    source = luigi.Parameter(default=None)

    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter(default=None)

    prefix = luigi.Parameter(default='git@github.com:', significant=False)

    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the ProcessTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        self.__logger = logger
        self._client = docker.client.from_env()
        
    def output(self):
        return MSTORE.get_target(self.name, 'process', 'data')

    def requires(self):
        my_dir = pathlib.Path(self.output().path).parent
        repo_path = pathlib.Path(self.subpath) / 'process'
        return dict(
                ingest = IngestTask(
                    self.name,
                    self.repo,
                    self.subpath,
                    self.branch,
                    self.prefix),
                pull = PullTask(
                    str(my_dir / 'pull'),
                    self.repo,
                    str(repo_path),
                    self.branch,
                    self.prefix)
                )
    
    def run(self):
        inputs = self.input()
        img, info = build_pulled(inputs['pull'], self.name, self._client, self.__logger)

        indata = (pathlib.Path(inputs['ingest'].path) / 'data').readlink()
        outdata = DSTORE.get_path(self.name, 'process')

        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
        container = self._client.containers.run(
            img,
            volumes=[
                f'{indata}:/indata:ro',
                f'{outdata}:/outdata'
                ],  # TODO: should we not depend on /data being where the container puts data?
            detach=True
        )
        
        for log in container.attach(stream=True):
            self.__logger.info(log.decode('utf-8'))
        
        with self.output().temporary_path() as tmpdir:
            root = pathlib.Path(tmpdir)
            root.mkdir(parents=True)

            os.symlink(outdata, root / 'data', target_is_directory=True)
            with open(root / 'info.json', 'wt') as out:
                out.write(json.dumps(info))
