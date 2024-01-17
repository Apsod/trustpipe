import os
import pathlib
import luigi
import argparse
import json
import re
import shutil
import jq
from dataclasses import dataclass
from collections import defaultdict, namedtuple
from trustpipe.target import CatalogTarget, catalog
from trustpipe.tasks import DataTarget, RepoTarget

class Cmd(object):
    cmd = None

    def __init__(self, args):
        self.__dict__.update(args.__dict__)

    @staticmethod
    def add_parsers(sp, *classes):
        for cls in classes:
            cls.__add_one(sp)

    @classmethod
    def __add_one(cls, sp):
        parser = sp.add_parser(cls.cmd)
        cls.mk_parser(parser)
        parser.set_defaults(__run=cls._run)

    @classmethod
    def mk_parser(cls, parser):
        pass

    @classmethod
    def _run(cls, args):
        obj = cls(args)
        obj.run()

    def run(self):
        pass


def scan_data():
    root = DataTarget.catalog_root()
    yield from os.scandir(root)

class ListCmd(Cmd):
    cmd = 'list'
    
    @classmethod
    def mk_parser(cls, parser):
        parser.add_argument('--filter', type=str)
        parser.add_argument('--parents', action='store_true')
        parser.add_argument('--children', action='store_true')

    @property
    def mk_graph(self):
        return self.parents or self.children

    def _filter(self, doc):
        if self.filter:
            if not hasattr(self, '__filter'):
                self.__filter = jq.compile(self.filter)
            res = self.__filter.input_text(doc).all()
            assert [type(r) for r in res] == [bool], "jq script must have single bool result per record"
            return res[0]
        else:
            return True

    def run(self):
        entries = os.scandir(DataTarget.catalog_root())
        for de in entries:
            doc = open(de).read()
            if self._filter(doc):
                print(de.path)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    Cmd.add_parsers(subparsers, ListCmd)
    
    args = parser.parse_args()
    args.__run(args)

if __name__ == '__main__':
    main()

