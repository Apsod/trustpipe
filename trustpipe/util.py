import json
import pathlib
import hashlib
import tempfile

from dataclasses import dataclass
from contextlib import contextmanager

import git
import docker

from slugify import slugify

"""
Utilities for generating slugs
"""

def slug(*args, **kwargs):
    return slugify(json.dumps([*args, kwargs]))

# This only works for python >= 3.6
def hashdigest(*args, **kwargs):
    bs = json.dumps(dict(args=args, kwargs=kwargs), separators=(',', ':')).encode('utf-8')
    return hashlib.new(
            'md5',
            bs
            ).hexdigest()
