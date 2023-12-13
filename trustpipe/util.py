import luigi
import os

class storage(luigi.Config):
    root = luigi.Parameter()

    def get_path(self, *path):
        return os.path.join(self.root, *path)

    def get_target(self, *path, **kwargs):
        return luigi.LocalTarget(self.get_path(*path), **kwargs)

class data_storage(storage):
    pass

class meta_storage(storage):
    pass
