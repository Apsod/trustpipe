from contextlib import contextmanager
from types import SimpleNamespace
from datetime import datetime
from dataclasses import dataclass
import json
import os
import logging

import luigi

logger = logging.getLogger('luigi-interface')

# This is mainly here for configuration: 
# catalog().get_target(path) returns a CatalogTarget 
# supported by an underlying LocalTarget @ catalog.root / *path
class catalog(luigi.Config):
    root = luigi.Parameter()
    
    def get_path(self, *path):
        return os.path.join(self.root, *path)

    def get_target(self, *path, **kwargs):
        return CatalogTarget(luigi.LocalTarget(self.get_path(*path), **kwargs))


def mk_taskinfo(task):
    item = {}
    item['task'] = task.get_task_family()
    item['args'] = task.to_str_params()
    item['storage'] = str(task.storage())
    if isinstance(task.input(), dict):
        item['catalog_deps'] = {k: v.read() for k, v in task.input().items() if isinstance(v, CatalogTarget)}
    else:
        item['catalog_deps'] = {}
    return item

"""
This is a convenience wrapper around a filesystem target (fs_target)
it shares the same existance criteria as the fs_target, but adds
functionality which simplifies working with it. The main logic
is within the `catalogize` function, which is intended to be used
as a with-statement. 

```

with ct.catalogize(task) as log:
    # On entering, task information, time stamps 
    # and other stuff is written to the log-dictionary.
    
    ... dostuff ...

    # We can use the log-dictionary inside the context
    log['note'] = 'this will be logged'

# On exiting the with-statement the log object is written to the fs_target
assert ct.exists()
```
"""
class CatalogTarget(luigi.Target):
    def __init__(self, fs_target):
        self.fs_target = fs_target

    @contextmanager
    def catalogize(self, task):
        assert not self.exists(), "Can't catalogize if already exists!"
        item = mk_taskinfo(task)
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
