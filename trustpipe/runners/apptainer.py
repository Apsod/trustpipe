import pathlib
import tempfile
import subprocess
from contextlib import contextmanager

def build(ref, dest):
    with ref.mk_context() as ctx:
        def_file = pathlib.Path() / ctx.path / 'task.def'
        result = subprocess.run(['apptainer', 'build', dest, def_file])
    return result

def run(image):
    subprocess.run(['apptainer', 'run', image])

import pull
build(pull.Reference('.'), '.')
