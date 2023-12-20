import luigi
from trustpipe import IngestTask, ProcessTask, DockerTask, PullTask


ingests = [
        dict(name='riksdag', repo='apsod/anforanden.git', branch='main'),
        ]

process = [
        dict(name='litbank', repo='apsod/litbank.git', branch='main')
        ]

class RunAll(luigi.WrapperTask):
    def requires(self):
        for kwargs in ingests:
            yield IngestTask(**kwargs)
        for kwargs in process:
            yield ProcessTask(**kwargs)
