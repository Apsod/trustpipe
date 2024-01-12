import json
import pathlib
import hashlib
import tempfile

from dataclasses import dataclass
from contextlib import contextmanager

import git

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
            'sha256',
            bs
            ).hexdigest()

"""
utilities for pulling, building, and handling git repos
"""

@dataclass
class repodata:
    path: str
    sha: str

@contextmanager
def get_repo(url, subpath, branch, dir=None, name=None):
    with tempfile.TemporaryDirectory(dir=dir, prefix=name) as tmpdir:
        root = pathlib.Path() / tmpdir / 'stuff'
        sha = clone_subpath(url, subpath, str(root), branch)
        subdir = str(root / subpath)
        yield repodata(subdir, sha)

def clone_subpath(url, subpath, dst, branch=None):
    OPTIONS = ['-n', '--depth=1', '--no-checkout', '--filter=tree:0']
    if branch:
        OPTIONS += [f'--branch={branch}']

    repo = git.Repo.clone_from(
            url, 
            multi_options=OPTIONS,
            to_path=dst)
    if subpath != '.':
        repo.git.sparse_checkout('init')
        repo.git.sparse_checkout(['set', '--no-cone', subpath])
    repo.git.checkout()
    return repo.rev_parse('HEAD').hexsha

def build_image(path, client, logger):
    logger.info(f'building image @ {path}')
    try:
        img, logs = client.images.build(path=path)
        for log in logs:
            logger.info(json.dumps(log))
    except docker.errors.APIError as e:
        logger.warning('docker API error during build: ' + e.explanation)
        raise e
    except docker.errors.BuildError as e:
        logger.warning('docker build error during build: ' + e.explanation)
        raise e
    return img
