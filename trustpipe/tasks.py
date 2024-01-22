import logging
import re
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
from luigi.util import requires

from trustpipe.target import CatalogTarget
from trustpipe.util import build_image, get_repo, slug, hashdigest


logger = logging.getLogger('luigi-interface')

###
# 
# Specifications
# 
###

@dataclass
class RepoSpec:
    ref: str = MISSING
    
    def to_task(self):
        return DockerTask(ref=self.ref, must_be_git=True)

@dataclass
class TaskSpec:
    name: str = MISSING
    kind: str = MISSING
    version: str = ""
    timestamp: str = ""
    modalities: str = ""
    data_explanation: str = ""
    data_source: str = ""
    copyright: str = ""
    author_name: str = ""
    author_email: str = ""
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
    @classmethod
    def catalog_root(cls):
        return super().catalog_root() / 'repos'

    def path(self):
        return self.get('storage')

    def spec(self):
        return TaskSpec.read(pathlib.Path() / self.path() / 'spec.yaml')

    def build_image(self, client, logger):
        return build_image(self.path(), client, logger)

    def dependencies(self):
        return self.spec().depends_on

gitpattern = re.compile(r'(?P<repo>git@.*:.*.git)#(?P<branch>.*):(?P<path>.*)')

class repostore(luigi.Config):
    store = luigi.Parameter()

class PullTask(luigi.Task):
    ref = luigi.Parameter()
    must_be_git = luigi.Parameter(default=False, visibility=ParameterVisibility.HIDDEN)
    
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the IngestTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)

        if (m := gitpattern.match(self.ref)):
            self.is_git = True
            self.repo = m.group('repo')
            self.branch = m.group('branch')
            self.path = m.group('path')
            self.slug = slug(self.repo, self.branch, self.path)
            self.hash = hashdigest(self.repo, self.branch, self.path)
        else:
            self.is_git = False
            self.path = str((pathlib.Path() / self.ref).absolute())
            self.slug = slug(self.path)
            self.hash = hashdigest(self.path)

        if self.must_be_git:
            assert self.is_git

    @property
    def basename(self):
        return f'{self.slug}_{self.hash}'

    def storage(self, absolute=True):
        fmt_mapping = dict(task=self)
        path = pathlib.Path() / repostore().store.format_map(fmt_mapping)
        if absolute:
            return path.absolute()
        else:
            return path

    def output(self):
        return RepoTarget.make(f'{self.basename}.json')

    def run(self):
        storage = self.storage()
        META = dict(
                task = self.get_task_family(),
                args = self.to_str_params(),
                storage = str(storage),
                )
        with self.output().catalogize(**META) as log:
            storage.parent.mkdir(parents=True, exist_ok=True)
            if self.is_git:
                with get_repo(self.repo, self.path, self.branch) as repo:
                    log['SHA'] = repo.sha
                    shutil.move(repo.path, str(storage))
            else:
                shutil.copytree(self.path, str(storage))

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
    @classmethod
    def catalog_root(cls):
        return super().catalog_root() / 'data'

    def path(self):
        return self.get('storage')

class datastore(luigi.Config):
    store = luigi.Parameter()

@requires(PullTask)
class DockerTask(luigi.Task):
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

        if (m := gitpattern.match(self.ref)):
            self.is_git = True
            self.repo = m.group('repo')
            self.branch = m.group('branch')
            self.path = m.group('path')
            self.slug = slug(self.repo, self.branch, self.path)
            self.hash = hashdigest(self.repo, self.branch, self.path)
        else:
            self.is_git = False
            self.path = str((pathlib.Path() / self.ref).absolute())
            self.slug = slug(self.path)
            self.hash = hashdigest(self.path)
    
    @property
    def basename(self):
        return f'{self.slug}_{self.hash}'

    def storage(self, spec, absolute=True):
        fmt_mapping = dict(spec=spec, task=self)
        path = pathlib.Path() / datastore().store.format_map(fmt_mapping)
        if absolute:
            return path.absolute()
        else:
            return path
        return path

    def output(self):
        return DataTarget.make(f'{self.basename}.json')

    def run(self):
        self.__logger.info('pulling repo')
        repo = self.input()
        spec = repo.spec()

        names = spec.depends_on.keys()
        trgs = yield [spec.depends_on[name].to_task() for name in names]

        storage = str(self.storage(spec))

        binds = [to_bind(storage, spec.output)]
        binds += [to_bind(trg.path(), f'/{name}', read_only=True) for name, trg in zip(names, trgs)]

        META = dict(
                task = self.get_task_family(),
                storage = storage,
                args = self.to_str_params(),
                spec = asdict(spec),
                pulled = repo.read(),
                upstreams = [trg.read() for trg in trgs],
                )

        with self.output().catalogize(**META) as log:
            img = repo.build_image(self._client, self.__logger)
            logger.info('removing repo')
            log['image'] = img.id
            log['binds'] = binds
            logger.info(json.dumps(log))
            #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
            logs = self._client.containers.run(
                img,
                name=self.slug,
                volumes=binds,  # TODO: should we not depend on /data being where the container puts data?
                stream=True,
                stdout=True,
                stderr=True,
            )
        
            for item in logs:
                self.__logger.info(item.decode('utf-8').rstrip())

def cleanup(task):
    logger.info('removing container')
    task._client.containers.get(task.slug)
    logger.info('removing repo')
    shutil.rmtree(task.input().path())
    task.input().fs_target.remove()

@DockerTask.event_handler(luigi.Event.SUCCESS)
def on_run_success(task):
    cleanup(task)


@DockerTask.event_handler(luigi.Event.FAILURE)
def on_run_failure(task, exception):
    cleanup(task)

###
#
# TASKS THAT WRAPS MULTIPLE REPOS
#
###

@requires(PullTask)
class WrapperTask(luigi.WrapperTask):
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

    def run(self):
        self.__logger.info('pulling repo')
        repo = self.input()
        spec = repo.spec()

        names = spec.depends_on.keys()
        self.__logger.info('running wrapped')
        trgs = yield [spec.depends_on[name].to_task() for name in names]

    def complete(self):
        return False
