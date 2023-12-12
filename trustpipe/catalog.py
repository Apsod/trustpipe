import logging
import json

from trustpipe.util import storage

import docker
import luigi

logger = logging.getLogger('luigi-interface')

class CatalogTask(luigi.Task):
    name = luigi.Parameter()
    path = luigi.Parameter()
    
    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the DockerTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        '''
        super().__init__(*args, **kwargs)
        self.__logger = logger

        self._client = docker.client.from_env()

    def output(self):
        return storage().get_target(f'{self.name}.img', format=luigi.format.NopFormat())
    
    def run(self):
        try:
            img, logs = self._client.images.build(path=self.path)
            for log in logs:
                self.__logger.info(json.dumps(log))
        except docker.errors.APIError as e:
            self.__logger.warning('docker API error during build: ' + e.explanation)
        except docker.errors.BuildError as e:
            self.__logger.warning('docker build error during build: ' + e.explanation)
        with self.output().open('wb') as f:
            for chunk in img.save():
                f.write(chunk)
