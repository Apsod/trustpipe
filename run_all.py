import luigi
from trustpipe import IngestTask, ProcessTask


ingests = [
        dict(name='riksdag', repo='apsod/anforanden.git', subpath='.', branch='small'),
        ]

process = [
        dict(name='litbank', repo='apsod/litbank.git', branch='small')
        ]

class RunAll(luigi.WrapperTask):
    def requires(self):
        for kwargs in ingests:
            yield IngestTask(**kwargs)
        for kwargs in process:
            yield ProcessTask(**kwargs)
