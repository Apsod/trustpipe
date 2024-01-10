import logging
import json
import os
import pathlib
import tempfile
import shutil

from contextlib import contextmanager
from dataclasses import dataclass, field, asdict, MISSING

import git
import docker

from omegaconf import OmegaConf

import luigi
from luigi.parameter import ParameterVisibility

from trustpipe.target import CatalogTarget
from trustpipe.util import build_image, get_repo, slughash


logger = logging.getLogger('luigi-interface')

###
# 
# Specifications
# 
###

@dataclass
class RepoSpec:
    repo: str = MISSING
    subpath: str = "."
    branch: str = "main"
    
    def to_task(self):
        return DockerTask(self.repo, self.subpath, self.branch)

@dataclass
class TaskSpec:
    name: str = MISSING
    kind: str = MISSING
    output: str = '/output'
    persist: bool = True
    depends_on: dict[str, RepoSpec] = field(default_factory=dict)

    @classmethod
    def read(cls, path):
        ret = OmegaConf.to_object(
                OmegaConf.merge(
                    OmegaConf.structured(cls), #schema
                    OmegaConf.load(path), #data
                )
            )
        return ret

###
#
# TASKS THAT PULLS REPOS
#
###

class RepoTarget(CatalogTarget):
    def path(self):
        return self.get('storage')

    def spec(self):
        return TaskSpec.read(pathlib.Path() / self.path() / 'spec.yaml')

    def build_image(self, client, logger):
        return build_image(self.path(), client, logger)

class PullTask(luigi.Task):
    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter("main")

    prefix = luigi.Parameter(
            default='git@github.com:', 
            significant=False, 
            visibility=ParameterVisibility.PRIVATE)

    store = luigi.Parameter()

    def basename(self, suffix=''):
        return slughash(self.repo, self.branch, self.subpath) + suffix

    def storage(self):
        return pathlib.Path() / self.store / self.basename()

    def output(self):
        return RepoTarget.make(str(pathlib.Path() / 'repos' / self.basename('.json')))

    def git_url(self):
        assert self.repo.endswith('.git'), 'repo must end with .git'
        assert not self.subpath.startswith('/'), 'subpaths must be relative'
        return f'{self.prefix}{self.repo}'

    def run(self):
        store = self.storage()
        META = dict(
                task = self.get_task_family(),
                args = self.to_str_params(),
                storage = str(store),
                )
        with self.output().catalogize(**META) as log:
            with get_repo(self.git_url(), self.subpath, self.branch) as repo:
                log['SHA'] = repo.sha
                store.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(repo.path, str(store))

###
#
# TASKS THAT RUNS SADs
#
###

def to_bind(host_path, container_path, read_only=False):
    parts = [host_path, container_path]
    if read_only:
        parts.append('ro')
    return ':'.join(parts)

class DataTarget(CatalogTarget):
    prefix = 'runs'
    def path(self):
        return self.get('storage')

class DockerTask(luigi.Task):
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

    def pull_task(self):
        return PullTask(self.repo, self.subpath, self.branch, self.prefix)

    def basename(self, suffix=''):
        return slughash(self.repo, self.branch, self.subpath) + suffix

    def storage(self):
        return pathlib.Path() / self.store / self.basename()

    def output(self):
        return DataTarget.make(str(pathlib.Path() / 'runs' / self.basename('.json')))

    def run(self):
        self.__logger.info('pulling repo')
        repo = yield self.pull_task()
        spec = repo.spec()

        names = spec.depends_on.keys()
        trgs = yield [spec.depends_on[name].to_task() for name in names]

        binds = [to_bind(str(self.storage()), spec.output)]
        binds += [to_bind(trg.path(), f'/{name}', read_only=True) for name, trg in zip(names, trgs)]

        META = dict(
                task = self.get_task_family(),
                storage = str(self.storage()),
                args = self.to_str_params(),
                **asdict(spec),
                )

        with self.output().catalogize(**META) as log:
            img = repo.build_image(self._client, self.__logger)
            log['image'] = img.id
            log['binds'] = binds
            logger.info(json.dumps(log))
            #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
            logs = self._client.containers.run(
                img,
                volumes=binds,  # TODO: should we not depend on /data being where the container puts data?
                stream=True,
                stdout=True,
                stderr=True,
            )
        
            for item in logs:
                self.__logger.info(item.decode('utf-8').rstrip())

