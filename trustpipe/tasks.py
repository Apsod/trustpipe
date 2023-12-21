import logging
import json
import os
import pathlib
import tempfile
from dataclasses import dataclass

import luigi
from luigi.parameter import ParameterVisibility
import docker

from trustpipe.target import catalog
from trustpipe.pull import PullTask

CATALOG = catalog()

logger = logging.getLogger('luigi-interface')

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
        return self.input()['pull'].get_store()

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
        with self.output().catalogize(self) as log:
            inputs = self.input()
            img = self.get_img()
            vbinds = [vbind.fmt() for vbind in self.volumes()]
            log['image'] = img.id
            log['vbinds'] = vbinds
            logger.info(json.dumps(log))
            #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
            logs = self._client.containers.run(
                img,
                volumes=vbinds,  # TODO: should we not depend on /data being where the container puts data?
                stream=True,
                stdout=True,
                stderr=True,
            )
        
            for item in logs:
                self.__logger.info(item.decode('utf-8').rstrip())

class RawIngestTask(DockerTask):
    def output(self):
        return Catalog.get_target('runs', self.name, ingest.json)

    def output_volume(self):
        return VBind(str(self.storage()), '/data')

    def requires(self):
        return {
                'pull': PullTask(self.repo, self.subpath, self.branch, self.prefix)
                }

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
    def output(self):
        return CATALOG.get_target('runs', self.name, 'process.json')

    def input_volumes(self):
        input_path = self.input()['ingest'].get_store()
        return [VBind(input_path, '/input')]

    def output_volume(self):
        return VBind(str(self.storage()), '/output')

    def requires(self):
        subpath = str(pathlib.Path() / self.subpath / 'process')
        return {
                'pull': PullTask(self.repo, subpath, self.branch, self.prefix),
                'ingest': IngestTask(self.name, self.repo, self.subpath, self.branch, self.prefix)
                }
