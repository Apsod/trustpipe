from contextlib import contextmanager
from types import SimpleNamespace
from datetime import datetime
from dataclasses import dataclass
import json
import os
import logging

import luigi

logger = logging.getLogger('luigi-interface')


class catalog(luigi.Config):
    root = luigi.Parameter()
    
    def get_path(self, *path):
        return os.path.join(self.root, *path)

    def get_target(self, *path, **kwargs):
        return CatalogTarget(luigi.LocalTarget(self.get_path(*path), **kwargs))

class CatalogTarget(luigi.Target):
    def __init__(self, fs_target):
        self.fs_target = fs_target

    @contextmanager
    def catalogize(self, task):
        assert not self.exists(), "Can't catalogize if already exists!"
        item = {}
        item['task'] = task.get_task_family()
        item['args'] = task.to_str_params()
        item['storage'] = str(task.storage())
        item['start'] = datetime.now().isoformat()
        item['inner'] = {}
        try:
            yield item['inner']
        except e:
            item['stop'] = datetime.now().isoformat()
            item['done'] = False
            item_str = json.dumps(item)
            logger.error(item_str)
            raise e
        else:
            item['stop'] = datetime.now().isoformat()
            item['done'] = True
            item_str = json.dumps(item)
            with self.fs_target.open('w') as h:
                h.write(item_str)


    def read(self):
        assert self.exists(), "Can't read if not already catalogized"
        with self.fs_target.open('r') as h:
            item = json.load(h)
        return item

    def get_store(self):
        return self.read()['storage']

    def exists(self):
        return self.fs_target.exists()
