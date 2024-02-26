import re
import pathlib
import tempfile
from dataclasses import dataclass
from contextlib import contextmanager

import git

gitpattern = re.compile(r'(?P<repo>git@.*:.*.git)#(?P<branch>.*):(?P<path>.*)')

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

@dataclass
class contextdata:
    path: str
    sha: str

class Reference(object):
    def __init__(self, ref):
        if (m := gitpattern.match(ref)):
            self.is_git = True
            self.repo = m.group('repo')
            self.branch = m.group('branch')
            self.path = m.group('path')
        else:
            self.is_git = False
            self.path = str((pathlib.Path() / ref).absolute())

    @contextmanager
    def mk_context(self):
        ctxman = self.mk_git_context if self.is_git else self.mk_local_context
        with ctxman() as ctx:
            yield ctx

    @contextmanager
    def mk_git_context(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = pathlib.Path() / tmpdir / 'stuff'
            sha = clone_subpath(self.repo, self.path, str(root), self.branch)
            subdir = str(root / self.path)
            yield contextdata(subdir, sha)

    @contextmanager
    def mk_local_context(self):
        yield contextdata(self.path, "N/A")