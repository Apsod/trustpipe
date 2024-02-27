import re
import pathlib
import tempfile
import shutil
from dataclasses import dataclass
from contextlib import contextmanager, nullcontext

from trustpipe.util import slug, hashdigest

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
    type: str

class Reference(object):
    def __init__(self, ref):
        if (m := gitpattern.match(ref)):
            self.is_git = True
            self.repo = m.group('repo')
            self.branch = m.group('branch')
            self.path = m.group('path')
            spec = (self.repo, self.branch, self.path)
        else:
            self.is_git = False

            path = (pathlib.Path() / ref).absolute()
            assert path.is_dir(), 'local path must exist and be directory'

            self.path = str(path)
            spec = (self.path,)

        self.slug = slug(*spec)
        self.hash = hashdigest(*spec)


    @contextmanager
    def mk_context(self, tmpdir=None):
        ctxman = self.mk_git_context(tmpdir) if self.is_git else self.mk_local_context(tmpdir)
        with ctxman as ctx:
            yield ctx

    @contextmanager
    def mk_git_context(self, tmpdir):
        ctxman = nullcontext(tmpdir) if tmpdir else tempfile.TemporaryDirectory(prefix='trustpipe-git')
        with ctxman as dir:
            root = pathlib.Path() / dir / 'stuff'
            sha = clone_subpath(self.repo, self.path, str(root), self.branch)
            subdir = str(root / self.path)
            yield contextdata(subdir, sha, 'git')

    @contextmanager
    def mk_local_context(self, tmpdir):
        ctxman = nullcontext(tmpdir) if tmpdir else tempfile.TemporaryDirectory(prefix='trustpipe-git')
        with ctxman as dir:
            root = pathlib.Path() / dir / 'stuff'
            sha = 'N/A'
            shutil.copytree(self.path, root)
            yield contextdata(root, "N/A", 'local')
