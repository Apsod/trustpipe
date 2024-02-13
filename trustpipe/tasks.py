import logging
import re
import json
import os
import pathlib
import tempfile
import shutil

from collections import OrderedDict
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
        return RunTask(ref=self.ref, must_be_git=True, storage_override=None)

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

def set_from_ref(task, ref):
    if (m := gitpattern.match(ref)):
        task.is_git = True
        task.repo = m.group('repo')
        task.branch = m.group('branch')
        task.path = m.group('path')

        spec = (task.repo, task.branch, task.path)

    else:
        task.is_git = False
        task.path = str((pathlib.Path() / ref).absolute())

        spec = (task.path, )

    task.slug = slug(*spec)
    task.hash = hashdigest(*spec)


class repostore(luigi.Config):
    store = luigi.Parameter()

class PullTask(luigi.Task):
    ref = luigi.Parameter()
    must_be_git = luigi.BoolParameter(default=False, visibility=ParameterVisibility.HIDDEN)
    
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the IngestTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        set_from_ref(self, self.ref)
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
class DataTask(luigi.Task):
    storage_override = luigi.OptionalParameter(visibility=ParameterVisibility.HIDDEN)
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the IngestTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        self._logger = logger
        self._cleanups = OrderedDict()
        set_from_ref(self, self.ref)

    @property
    def basename(self):
        return f'{self.slug}_{self.hash}'

    def storage(self, spec, absolute=True):
        if self.storage_override is not None:
            path = pathlib.Path() / self.storage_override
        
        else:
            fmt_mapping = dict(spec=spec, task=self)
            path = pathlib.Path() / datastore().store.format_map(fmt_mapping)

        if absolute:
            return path.absolute()
        return path

    def output(self):
        return DataTarget.make(f'{self.basename}.json')

    def add_cleanup(self, name, function):
        self._cleanups[name] = function
    
    def on_success(self):
        while self._cleanups:
            name, fun = self._cleanups.popitem(last=False)
            self._logger.info(f'cleanup [{name}] ....')
            fun()
            self._logger.info(f'cleanup [{name}] DONE')

    def on_failure(self, exception):
        self.on_success()
        return str(exception)


class RunTask(DataTask):
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the IngestTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        self._client = docker.client.from_env()
    
    def run(self):
        self.add_cleanup('repo catalog', mk_repo_catalog_cleanup(self))
        repo = self.input()
        spec = repo.spec()
        self.add_cleanup('repo store', mk_repo_store_cleanup(self))

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
            img = repo.build_image(self._client, logger)
            log['image'] = img.id
            log['binds'] = binds
            log['logs'] = []
            self._logger.info(json.dumps(log))
            #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
            logs = self._client.containers.run(
                img,
                name=self.slug,
                volumes=binds,
                stream=True,
                stdout=True,
                stderr=True,
            )

            self.add_cleanup('container', mk_container_cleanup(self))
         
            for item in logs:
                item = item.decode('utf-8').rstrip()
                log['logs'].append(item)
                self._logger.info(item)


class MockTask(DataTask):
    def run(self):
        self.add_cleanup('repo catalog', mk_repo_catalog_cleanup(self))
        repo = self.input()
        spec = repo.spec()
        self.add_cleanup('repo store', mk_repo_store_cleanup(self))

        names = spec.depends_on.keys()
        storage = str(self.storage(spec))
        META = dict(
                task = self.get_task_family(),
                storage = storage,
                args = self.to_str_params(),
                spec = asdict(spec),
                pulled = repo.read(),
                )
        with self.output().catalogize(**META) as log:
            pass


def mk_container_cleanup(task):
    client = task._client
    name = task.slug
    return lambda: client.containers.get(name).remove()

def mk_repo_store_cleanup(task):
    path = task.input().path()
    return lambda: shutil.rmtree(path)

def mk_repo_catalog_cleanup(task):
    trg = task.input().fs_target
    return lambda: trg.remove()
