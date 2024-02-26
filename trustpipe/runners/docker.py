import pathlib
import tempfile
import subprocess
from contextlib import contextmanager

def build(ref, dest):
    with ref.mk_context() as ctx:
        def_file = pathlib.Path() / ctx.path
        result = subprocess.run(['docker', 'build', '-o', f'type=tar,dest={dest}', def_file])
    return result


import pull
build(pull.Reference('/home/amaru/git/trustpipe/example/ingest/'), 'img.tar')
