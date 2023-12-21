import logging
import os
import pathlib
import tempfile
import shutil
from contextlib import contextmanager
from dataclasses import dataclass

import luigi
from luigi.parameter import ParameterVisibility
import git

from trustpipe.target import catalog

CATALOG = catalog()

logger = logging.getLogger('luigi-interface')

def clone_subpath(url, subpath, dst, branch=None):
    OPTIONS = ['-n', '--depth=1', '--no-checkout', '--filter=tree:0']
    if branch:
        OPTIONS += [f'--branch={branch}']

    repo = git.Repo.clone_from(
            url, 
            multi_options=OPTIONS,
            to_path=dst)
    repo.git.sparse_checkout('init')
    repo.git.sparse_checkout(['set', '--no-cone', subpath])
    repo.git.checkout()
    return repo.rev_parse('HEAD').hexsha

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
###
#
# PULLING STUFF FROM GITHUB
#
###
"""
A luigi task whose job it is to pull (sub)-repos from github.
"""
class PullTask(luigi.Task):
    repo = luigi.Parameter()
    subpath = luigi.Parameter(".")
    branch = luigi.Parameter("main")

    prefix = luigi.Parameter(
            default='git@github.com:', 
            significant=False, 
            visibility=ParameterVisibility.PRIVATE)

    store = luigi.Parameter()

    def storage(self):
        return pathlib.Path() / self.store / self.repo / self.branch / self.subpath

    def output(self):
        return CATALOG.get_target('pulled', self.repo, self.branch, self.subpath + '.json')

    def git_url(self):
        assert self.repo.endswith('.git'), 'repo must end with .git'
        assert not self.subpath.startswith('/'), 'subpaths must be relative'
        return f'{self.prefix}{self.repo}'

    def run(self):
        with self.output().catalogize(self) as log:
            store = self.storage()
            
            with get_repo(self.git_url(), self.subpath, self.branch) as repo:
                store.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(repo.path, str(store))
                log['SHA'] = repo.sha
