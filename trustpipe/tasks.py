import logging
import json
import secrets
import os
import pathlib
import tempfile
import re
from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass

import luigi
from luigi.parameter import ParameterVisibility
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

@dataclass
class repodata:
    path: str
    sha: str

@contextmanager
def get_repo(url, subpath, branch, dir=None, name=None):
    with tempfile.TemporaryDirectory(dir=dir, prefix=name) as tmpdir:
        root = pathlib.Path() / tmpdir / 'stuff'
        sha = clone_subpath(url, subpath, str(root), branch)
        subdir = str(root / subpath)
        yield repodata(subdir, sha)

###
#
# PULLING STUFF FROM GITHUB
#
###

class PullTask(luigi.Task):
    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter("main")

    prefix = luigi.Parameter(
            default='git@github.com:', 
            significant=False, 
            visibility=ParameterVisibility.PRIVATE)

    def output(self):
        return MSTORE.get_target(str(pathlib.Path() / 'pulled' / self.repo / self.branch / self.subpath))

    def git_url(self):
        assert self.repo.endswith('.git'), 'repo must end with .git'
        assert not self.subpath.startswith('/'), 'subpaths must be relative'
        return f'{self.prefix}{self.repo}'

    def run(self):
        INFO = {}
        INFO['task'] = 'PullTask'
        INFO['args'] = self.to_str_params()
        INFO['start'] = datetime.now().isoformat()
        out = self.output()
        out_path = pathlib.Path() / out.path
        tmpdir = out_path.parent
        rng = secrets.token_hex(10)
        tmpname = f'{out_path.name}-tmp-{rng}'

        tmpdir.mkdir(parents=True, exist_ok=True)

        with get_repo(self.git_url(), self.subpath, self.branch, dir=tmpdir, name=tmpname) as repo:
            os.rename(repo.path, out_path)
            INFO['SHA'] = repo.sha
            INFO['stop'] = datetime.now().isoformat()

        logger.info(json.dumps(INFO))


def build_pulled(loc, name, client, logger):
    logger.info(f'building image @ {loc}')
    try:
        img, logs = client.images.build(path=loc)
        for log in logs:
            logger.info(json.dumps(log))
    except docker.errors.APIError as e:
        logger.warning('docker API error during build: ' + e.explanation)
        raise e
    except docker.errors.BuildError as e:
        logger.warning('docker build error during build: ' + e.explanation)
        raise e
    return img

@dataclass
class VBind:
    hostpath: str
    contpath: str
    read_only: bool = False

    def fmt(self) -> str:
        parts = [self.hostpath, self.contpath]
        if self.read_only:
            parts.append('ro')
        return ':'.join(parts)

class DockerTask(luigi.Task):
    name = luigi.Parameter()


    # DOCKER REPO Parameters
    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter("main")

    prefix = luigi.Parameter(
            default='git@github.com:', 
            significant=False, 
            visibility=ParameterVisibility.PRIVATE)

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
        raise NotImplementedError()

    def requires(self):
        return {'pull': PullTask(self.repo, self.subpath, self.branch, self.prefix)}

    def repo_location(self):
        return self.input()['pull'].path

    def get_img(self):
        return build_pulled(self.repo_location(), self.name, self._client, self.__logger)

    def input_volumes(self) -> [VBind]:
        return []

    def output_volume(self) -> VBind:
        raise NotImplementedError()

    def other_volumes(self) -> [VBind]:
        return []

    def volumes(self):
        binds = [self.output_volume()]
        binds += self.input_volumes()
        binds += self.other_volumes()
        return binds

    def run(self):
        inputs = self.input()
        img = self.get_img()
        vbinds = [vbind.fmt() for vbind in self.volumes()]
        INFO = {}
        INFO['task'] = self.get_task_family()
        INFO['args'] = self.to_str_params()
        INFO['start'] = datetime.now().isoformat()
        INFO['image'] = img.id
        INFO['output'] = self.output_volume().hostpath
        INFO['vbinds'] = vbinds
        self.__logger.info(json.dumps(INFO))
        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
        container = self._client.containers.run(
            img,
            volumes=vbinds,  # TODO: should we not depend on /data being where the container puts data?
            detach=True
        )
        
        for log in container.attach(stream=True):
            self.__logger.info(log.decode('utf-8'))
        
        INFO['stop'] = datetime.now().isoformat()
        with self.output().open('w') as handle:
            json.dump(INFO, handle)


class IngestTask(DockerTask):

    def output(self):
        return MSTORE.get_target(self.name, 'ingest')

    def output_volume(self):
        return VBind(DSTORE.get_path(self.name, 'ingest'), '/data')

    def requires(self):
        subpath = str(pathlib.Path() / self.subpath / 'ingest')
        return {
                'pull': PullTask(self.repo, subpath, self.branch, self.prefix)
                }

class ProcessTask(DockerTask):
    source = luigi.Parameter(default=None)

    def output(self):
        return MSTORE.get_target(self.name, 'process')
    
    def input_name(self):
        return self.source if self.source else self.name

    def input_volumes(self):
        return [VBind(DSTORE.get_path(self.input_name(), 'ingest'), '/input')]

    def output_volume(self):
        return VBind(DSTORE.get_path(self.name, 'process'), '/output')

    def requires(self):
        subpath = str(pathlib.Path() / self.subpath / 'process')
        return {
                'pull': PullTask(self.repo, subpath, self.branch, self.prefix),
                'ingest': IngestTask(self.input_name(), self.repo, self.subpath, self.branch, self.prefix)
                }


###
#
# RUNNING PULLED INGEST DOCKERS
#
###
#
#class IngestTask(luigi.Task):
#    name = luigi.Parameter()
#
#    repo = luigi.Parameter()
#    subpath = luigi.Parameter(".")
#    branch = luigi.Parameter(default=None)
#
#    prefix = luigi.Parameter(
#            default='git@github.com:', 
#            significant=False, 
#            visibility=ParameterVisibility.PRIVATE)
#    
#    def __init__(self, *args, **kwargs):
#        '''
#        When a new instance of the IngestTask class gets created:
#        - call the parent class __init__ method
#        - start the logger
#        - init an instance of the docker client
#        '''
#        super().__init__(*args, **kwargs)
#        self.__logger = logger
#        self._client = docker.client.from_env()
#        
#    def output(self):
#        return MSTORE.get_target(self.name, 'ingest', 'data')
#
#    def requires(self):
#        my_dir = pathlib.Path(self.output().path).parent
#        repo_path = pathlib.Path(self.subpath) / 'ingest'
#
#        return PullTask(
#                str(my_dir / 'pull'),
#                self.repo,
#                str(repo_path),
#                self.branch,
#                self.prefix)
#    
#    def run(self):
#        img, info = build_pulled(self.input(), f'{self.name}.ingest', self._client, self.__logger)
#
#        host_path = DSTORE.get_path(self.name, 'ingest')
#
#        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
#
#        container = self._client.containers.run(
#            img,
#            volumes=[f'{host_path}:/data'],  # TODO: should we not depend on /data being where the container puts data?
#            detach=True
#        )
#        
#        for log in container.attach(stream=True):
#            self.__logger.info(log.decode('utf-8'))
#        
#        with self.output().temporary_path() as tmpdir:
#            root = pathlib.Path(tmpdir)
#            root.mkdir(parents=True)
#
#            os.symlink(host_path, root / 'data', target_is_directory=True)
#            with open(root / 'info.json', 'wt') as out:
#                out.write(json.dumps(info))
#
####
##
## RUNNING PULLED PROCESS DOCKERS
##
####
#
#class ProcessTask(luigi.Task):
#    name = luigi.Parameter()
#    source = luigi.Parameter(default=None)
#
#    repo = luigi.Parameter()
#    subpath = luigi.Parameter(".")
#    branch = luigi.Parameter(default=None)
#
#    prefix = luigi.Parameter(
#            default='git@github.com:', 
#            significant=False, 
#            visibility=ParameterVisibility.PRIVATE)
#
#    def __init__(self, *args, **kwargs):
#        '''
#        When a new instance of the ProcessTask class gets created:
#        - call the parent class __init__ method
#        - start the logger
#        - init an instance of the docker client
#        '''
#        super().__init__(*args, **kwargs)
#        self.__logger = logger
#        self._client = docker.client.from_env()
#        
#    def output(self):
#        return MSTORE.get_target(self.name, 'process', 'data')
#
#    def requires(self):
#        my_dir = pathlib.Path(self.output().path).parent
#        source = self.source if self.source else self.name
#        repo_path = pathlib.Path(self.subpath) / 'process'
#        return dict(
#                ingest = IngestTask(
#                    self.name,
#                    self.repo,
#                    self.subpath,
#                    self.branch,
#                    self.prefix),
#                pull = PullTask(
#                    str(my_dir / 'pull'),
#                    self.repo,
#                    str(repo_path),
#                    self.branch,
#                    self.prefix)
#                )
#    
#    def run(self):
#        inputs = self.input()
#        img, info = build_pulled(inputs['pull'], f'{self.name}.process', self._client, self.__logger)
#
#        indata = (pathlib.Path(inputs['ingest'].path) / 'data').readlink()
#        outdata = DSTORE.get_path(self.name, 'process')
#
#        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
#        container = self._client.containers.run(
#            img,
#            volumes=[
#                f'{indata}:/indata:ro',
#                f'{outdata}:/outdata'
#                ],  # TODO: should we not depend on /data being where the container puts data?
#            detach=True
#        )
#        
#        for log in container.attach(stream=True):
#            self.__logger.info(log.decode('utf-8'))
#        
#        with self.output().temporary_path() as tmpdir:
#            root = pathlib.Path(tmpdir)
#            root.mkdir(parents=True)
#
#            os.symlink(outdata, root / 'data', target_is_directory=True)
#            with open(root / 'info.json', 'wt') as out:
#                out.write(json.dumps(info))
