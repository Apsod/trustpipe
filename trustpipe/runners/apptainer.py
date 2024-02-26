import pathlib
import os
import tempfile
import subprocess
from contextlib import contextmanager, chdir

@contextmanager
def build(ref, dest):
    with ref.mk_context() as ctx:
        path = (pathlib.Path() / ctx.path).absolute()
        with chdir(path): 
            result = subprocess.run(['apptainer', 'build', dest, 'task.def'])
            yield result


def run(image):
    subprocess.run(['apptainer', 'run', image])


def build_and_run(ref):
    with tempfile.TemporaryDirectory() as tmpdir:
        root = pathlib.Path() / tmpdir
        build(ref, str(root / 'img'))
        run(root / 'img')

import pull

ref = pull.Reference('/home/amaru/git/trustpipe/example/ingest')
ref = pull.Reference('git@github.com:apsod/trustpipe.git#singularity:example/ingest')

with build(ref, 'task.sif') as b:
    print(b)
