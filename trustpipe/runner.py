import shutil
import logging
import json
import pathlib
import subprocess
import tempfile
from contextlib import chdir, contextmanager

logger = logging.getLogger(f'luigi-interface.{__name__}')

def execute(cmd, logger, **kwargs):
    logger.info(f'running: {cmd}')
    popen = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            **kwargs)
    for line in iter(popen.stdout.readline, ""):
        line = line.strip()
        logger.debug(line)
    popen.stdout.close()

    retcode = popen.wait()
    if retcode:
        logger.error(f'failed: {cmd}')
        raise subprocess.CalledProcessError(retcode, cmd)
    logger.info(f'finished: {cmd}')

class Runner(object):
    def __init__(self, logger):
        self.logger = logger
    
    @contextmanager
    def build(self, ctx):
        pass
    
    @contextmanager
    def run(self, image, binds, variables):
        pass

    def run_and_build(self, ctx, binds, variables):
        self.logger.info('starting build')
        with self.build(ctx) as img:
            self.logger.info('starting run')
            self.run(img, binds, variables)
            self.logger.info('finished run')
        self.logger.info('finished build and run')


class ApptainerRunner(Runner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cmd = shutil.which('apptainer')
    
    @contextmanager
    def build(self, ctx):
        path = (pathlib.Path() / ctx.path).absolute()
        with tempfile.TemporaryDirectory(prefix='trustpipe-apptainer') as tmpdir:
            dest = (pathlib.Path() / tmpdir / 'task.sif').absolute()
            with chdir(path):
                execute([self.cmd, 'build', dest, 'task.def'])
            yield dest

    def run(self, image, binds, variables):
        if variables:
            env_args = ['--env', ','.join([f'{key}={val}' for key, val in variables.items()])]
        else:
            env_args = []

        execute([
            self.cmd, 'run', 
            *[f'-B={bind}' for bind in binds], 
            *env_args,
            image,
            ],
            self.logger)


class DockerRunner(Runner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import docker
        self.client = docker.client.from_env()
    
    @contextmanager
    def build(self, ctx):
        import docker
        path = (pathlib.Path() / ctx.path).absolute()
        try:
            img, logs = self.client.images.build(path=str(path))
            for log in logs:
                self.logger.info(json.dumps(log))
        except docker.errors.APIError as e:
            self.logger.warning('docker API error during build: ' + e)
            raise e
        except docker.errors.BuildError as e:
            self.logger.warning('docker build error during build: ' + e)
            raise e
        yield img
        #logger.info('removing image')
        #img.remove()
    
    def run(self, image, binds, variables):
        container = self.client.containers.run(
                image,
                volumes=binds,
                environment=variables,
                detach=True,
                )
        
        for l in container.logs(stream=True, since=1):
            l = l.decode('utf-8').strip()
            self.logger.info(l)
        
        #logger.info('removing container')
        #container.remove()
