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


from trustpipe.util import catalog


CATALOG = catalog()

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

@contextmanager
def log_inner(task):
    INFO = mk_task_info(self)
    INFO['start'] = datetime.now().isoformat()
    yield INFO
    INFO['stop'] = datetime.now().isoformat()

def mk_task_info(task):
    return dict(
            task = task.get_task_family(),
            args = task.to_str_params(),
            storage = str(task.storage())
            )

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

    store = luigi.Parameter()

    def storage(self):
        return pathlib.Path() / self.store / self.repo / self.branch / self.subpath

    def output(self):
        return CATALOG.get_target('pulled', self.repo, self.branch, self.subpath + '.json')

    def git_url(self):
        assert self.repo.endswith('.git'), 'repo must end with .git'
        assert not self.subpath.startswith('/'), 'subpaths must be relative'
        return f'{self.prefix}{self.repo}'

    def run(self):
        INFO = mk_task_info(self)
        INFO['start'] = datetime.now().isoformat()

        store = self.storage()
        store.mkdir(parents=True, exist_ok=True)
        rng = secrets.token_hex()
        
        with get_repo(self.git_url(), self.subpath, self.branch, dir=store.parent, name=f'{store.name}-tmp-{rng}') as repo:
            os.rename(repo.path, str(self.storage()))
            INFO['SHA'] = repo.sha
        
        INFO['stop'] = datetime.now().isoformat()
        with self.output().open('w') as handle:
            json.dump(INFO, handle)


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

###
#
# RUNNING DOCKERS
#
###

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

    store = luigi.Parameter()

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

    def storage(self):
        return pathlib.Path() / self.store / self.name

    def output(self):
        raise NotImplementedError()

    def requires(self):
        return {'pull': PullTask(self.repo, self.subpath, self.branch, self.prefix)}

    def repo_location(self):
        return json.load(self.input()['pull'].open('r'))['storage']

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
        INFO = mk_task_info(self)
        INFO['start'] = datetime.now().isoformat()
        inputs = self.input()
        img = self.get_img()
        vbinds = [vbind.fmt() for vbind in self.volumes()]
        INFO['image'] = img.id
        INFO['vbinds'] = vbinds
        self.__logger.info(json.dumps(INFO))
        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
        container = self._client.containers.run(
            img,
            volumes=vbinds,  # TODO: should we not depend on /data being where the container puts data?
            environment=['HF_TOKEN'],
            detach=True
        )
        
        for log in container.attach(stream=True):
            self.__logger.info(log.decode('utf-8'))
        
        INFO['stop'] = datetime.now().isoformat()
        with self.output().open('w') as handle:
            json.dump(INFO, handle)


class IngestTask(DockerTask):
    def output(self):
        return CATALOG.get_target('runs', self.name, 'ingest.json')

    def output_volume(self):
        return VBind(str(self.storage()), '/data')

    def requires(self):
        subpath = str(pathlib.Path() / self.subpath / 'ingest')
        return {
                'pull': PullTask(self.repo, subpath, self.branch, self.prefix)
                }

class ProcessTask(DockerTask):
    source = luigi.Parameter(default=None)

    def output(self):
        return CATALOG.get_target('runs', self.name, 'process.json')
    
    def input_name(self):
        return self.source if self.source else self.name

    def input_volumes(self):
        input_path = json.load(self.input()['ingest'].open('r'))['storage']
        return [VBind(input_path, '/input')]

    def output_volume(self):
        return VBind(str(self.storage()), '/output')

    def requires(self):
        subpath = str(pathlib.Path() / self.subpath / 'process')
        return {
                'pull': PullTask(self.repo, subpath, self.branch, self.prefix),
                'ingest': IngestTask(self.input_name(), self.repo, self.subpath, self.branch, self.prefix)
                }
