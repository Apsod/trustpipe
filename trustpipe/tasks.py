import logging
import re
import json
import os
import pathlib
import tempfile
import shutil

from collections import OrderedDict
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field, asdict, MISSING

import git
import docker

from omegaconf import OmegaConf

import luigi
from luigi.parameter import ParameterVisibility
from luigi.util import requires

from trustpipe.target import CatalogTarget
from trustpipe.util import slug, hashdigest

from trustpipe.runner import ApptainerRunner, DockerRunner
from trustpipe.pull import Reference

logger = logging.getLogger('luigi-interface')

    
def to_bind(host_path, container_path, read_only=False):
    parts = [host_path, container_path]
    if read_only:
        parts.append('ro')
    return ':'.join(parts)


@dataclass
class DependencySpec:
    ref: str = MISSING
    at: str = ""


@dataclass
class TaskSpec:
    name: str = MISSING
    kind: str = MISSING
    version: str = ""
    modalities: str = ""
    description: str = ""
    data_source: str = ""
    copyright: str = ""
    author_name: str = ""
    author_email: str = ""
    output: str = '/output'
    persist: bool = True
    depends_on: dict[str, DependencySpec] = field(default_factory=dict)

    @classmethod
    def read(cls, path):
        ret = OmegaConf.to_object(
                OmegaConf.merge(
                    OmegaConf.structured(cls), #schema
                    OmegaConf.load(path), #data
                )
            )
        return ret
    
class DataTarget(CatalogTarget):
    @classmethod
    def catalog_root(cls):
        return super().catalog_root() / 'data'

    def path(self):
        return self.get('storage')

class datastore(luigi.Config):
    store = luigi.Parameter()

class BaseTask(luigi.Task):
    storage_override = luigi.OptionalStrParameter(visibility=ParameterVisibility.HIDDEN)
    reference = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ref = Reference(self.reference)

    @property
    def basename(self):
        return f'{self.ref.slug}_{self.ref.hash}'

    def storage(self, absolute=True, make=True):
        if self.storage_override is not None:
            path = pathlib.Path() / self.storage_override
        else:
            path = pathlib.Path() / datastore().store / self.basename

        if absolute:
            path = path.absolute()
        
        if make:
            path.mkdir(parents=True, exist_ok=True)

        return path

    def output(self):
        return DataTarget.make(f'{self.basename}.json')

RUNNERDICT = dict(
        apptainer=ApptainerRunner,
        docker=DockerRunner
        )

class RunTask(BaseTask):
    runner_type=luigi.ChoiceParameter(choices=list(RUNNERDICT))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runner = RUNNERDICT.get(self.runner_type)()

    def run(self):
        logger.info('making build context')
        with self.ref.mk_context() as ctx:
            logger.info(ctx.path)
            spec = TaskSpec.read(pathlib.Path() / ctx.path / 'spec.yaml')
            
            names = spec.depends_on.keys()
            logger.info('figuring out dependencies')
            trgs = yield [RunTask(reference=spec.depends_on[name].ref, storage_override=None) for name in names]
            
            storage = str(self.storage())

            binds = [to_bind(storage, spec.output)]

            for name, trg in zip(names, trgs):
                dst = spec.depends_on[name].at
                dst = dst if dst else f'/{name}'

                binds.append(to_bind(trg.path(), dst, read_only=True))
            
            META = dict(
                    task = self.get_task_family(),
                    storage = storage,
                    args = self.to_str_params(),
                    spec = asdict(spec),
                    sha = ctx.sha,
            )

            with self.output().catalogize(**META) as log:
                self.runner.run_and_build(ctx, binds)

class MockTask(BaseTask):
    pull = luigi.BoolParameter()
    def run(self): 

        if self.pull:
            with self.ref.mk_context():
                spec = TaskSpec.read(pathlib.Path() / ctx.path / 'spec.yaml')
                spec = asdict(spec)
        else:
            spec = {}

        storage = str(self.storage())

        META = dict(
                task = self.get_task_family(),
                storage = storage,
                args = self.to_str_params(),
                spec = spec,
                sha = ctx.sha,
                )

        with self.output().catalogize(**META) as log:
            pass

