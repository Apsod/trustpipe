import re
import pathlib
import tempfile
import shutil
import logging
from dataclasses import dataclass
from contextlib import contextmanager, nullcontext

from trustpipe.util import slug, hashdigest

import luigi
import git

logger = logging.getLogger(f'luigi-interface.{__name__}')

gitpattern = re.compile(r'(?P<loc>.*):(?P<repo>.*.git)#(?P<branch>.*):(?P<path>.*)')

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

class gitconfig(luigi.Config):
    pat = luigi.Parameter(default='', visibility=luigi.parameter.ParameterVisibility.PRIVATE)

@dataclass
class contextdata:
    path: str
    sha: str
    type: str

class Reference(object):
    def __init__(self, ref):
        if (m := gitpattern.match(ref)):
            self.is_git = True
            self.loc = m.group('loc')
            self.repo = m.group('repo')
            self.branch = m.group('branch')
            self.path = m.group('path')
            spec = (self.loc, self.repo, self.branch, self.path)
        else:
            self.is_git = False

            path = (pathlib.Path() / ref).absolute()
            assert path.is_dir(), 'local path must exist and be directory'

            self.path = str(path)
            spec = (self.path,)

        self.slug = slug(*spec)
        self.hash = hashdigest(*spec)

    def mk_git_url(self):
        conf = gitconfig()

        if conf.pat:
            url = f'https://{conf.pat}@{self.loc}/{self.repo}'
        else:
            url = f'git@{self.loc}:{self.repo}'
        return url



    @contextmanager
    def mk_context(self, tmpdir=None):
        ctxman = self.mk_git_context(tmpdir) if self.is_git else self.mk_local_context(tmpdir)
        with ctxman as ctx:
            yield ctx

    @contextmanager
    def mk_git_context(self, tmpdir):
        ctxman = nullcontext(tmpdir) if tmpdir else tempfile.TemporaryDirectory(prefix='trustpipe-git')
        git_url = self.mk_git_url()
        with ctxman as dir:
            root = pathlib.Path() / dir / 'stuff'
            sha = clone_subpath(git_url, self.path, str(root), self.branch)
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
