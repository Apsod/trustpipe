import luigi
from trustpipe.ingest import IngestTask

datasets = {
        'riksdag': 'github.com/Apsod/anforanden.git#main:ingest',
        'hplt': 'github.com/Apsod/hplt.git#main:ingest',
        }

class IngestAll(luigi.WrapperTask):
    def requires(self):
        for name, path in datasets.items():
            yield IngestTask(name, path)
