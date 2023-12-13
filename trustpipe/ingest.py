import logging

from trustpipe.catalog import CatalogTask
from trustpipe.util import data_storage, meta_storage

import luigi
import docker
import os

logger = logging.getLogger('luigi-interface')

DATA_STORE = data_storage()
META_STORE = meta_storage()

class IngestTask(luigi.Task):
    name = luigi.Parameter()
    path = luigi.Parameter()
    
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
        
    def requires(self):
        return CatalogTask(self.name, self.path)
        
    def output(self):
        return META_STORE.get_target(f'{self.name}/data')
        
    def run(self):
        with self.input().open('r') as img_file:
            img, = self._client.images.load(img_file)
        self.__logger.info(str(img))
            
        host_path = DATA_STORE.get_path(self.name)
        
        #TODO: Add possibility to mount cache volumes? or other kinds of volumes? (Is it needed?)
        container = self._client.containers.run(
            img, 
            volumes=[f'{host_path}:/data'],  # TODO: should we not depend on /data being where the container puts data?
            detach=True
        )
        
        for log in container.attach(stream=True):
            self.__logger.info(log.decode('utf-8'))
        
        os.symlink(host_path, self.output().path, target_is_directory=True)
