from contextlib import contextmanager
from types import SimpleNamespace
from datetime import datetime
from dataclasses import dataclass
import pathlib
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
    
    do_stuff()

    # We can use the log-dictionary inside the context
    log['note'] = 'this will be logged'

do_stuff()
```

On exiting the with-statement the log object is written to the fs_target
(And consequently, the target will *exist* after exiting the with-statement)
"""
class CatalogTarget(luigi.Target):
    def __init__(self, fs_target):
        self.fs_target = fs_target

    @classmethod
    def catalog_root(cls):
        return pathlib.Path() / catalog().root

    @classmethod
    def make(cls, path, **kwargs):
        fspath = str(cls.catalog_root() / path)
        target = luigi.LocalTarget(fspath, **kwargs)
        return cls(target)

    @contextmanager
    def catalogize(self, **kwargs):
        assert not self.exists(), "Can't catalogize if already exists!"
        item = {}
        item.update(kwargs)
        item['duration'] = {}
        item['duration']['start'] = datetime.now().isoformat()
        try:
            yield item
        except Exception as e:
            item['duration']['stop'] = datetime.now().isoformat()
            item['done'] = False
            item_str = json.dumps(item)
            logger.error(item_str)
            raise e
        else:
            item['duration']['stop'] = datetime.now().isoformat()
            item['done'] = True
            item_str = json.dumps(item)
            with self.fs_target.open('w') as h:
                h.write(item_str)
    
    def read(self):
        assert self.exists(), "Can't read if not already catalogized"
        with self.fs_target.open('r') as h:
            item = json.load(h)
        return item

    def get(self, index, default=None):
        return self.read().get(index, default)

    def exists(self):
        return self.fs_target.exists()

