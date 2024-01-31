"""Command Line Interface of trustpipe"""

import os
import shutil
import pathlib
import json
from typing import Optional, List
from dataclasses import dataclass

from trustpipe import DataTarget, RunTask, MockTask, RepoTarget

import jq
import luigi
import click

#################################
# HELPERS
#################################
class JsonFilter(object):
    """class that composes and applies multiple filters to json documents"""

    def __init__(self, jq_filters: List[str]) -> None:
        """
        Args:
            jq_filters: e.g. ['.spec.kind = "process"', '.spec.name = "litteraturbanken"']
        """
        self.filters = [jq.compile(jq_filter) for jq_filter in jq_filters]

    def apply(self, doc: str) -> bool:
        """
        Args:
            doc: e.g. {"task": "DockerTask", "spec": {"name": "litteraturbanken", "kind": "process", ..}, ..}

        Returns:
            keep_doc: whether to keep the input doc or not.
        """
        for _filter in self.filters:
            res = _filter.input_text(doc).all()
            assert [type(r) for r in res] == [bool], "jq script must have single bool result per record"
            if res[0] is False:
                return False
        return True


#################################
# CLI
#################################
@click.group()
def main() -> None:
    """CLI FOR TRUSTPIPE"""
    pass


#################################
# CLI: run
#################################
@main.command(name="run")
@click.argument('ref', nargs=-1)
@click.option(
    '--workers',
    type=int,
    required=False,
    help="Numbers of concurrent tasks to run. Defaults to the number of REFs supplied.")
@click.option(
    '--must-be-git/--can-be-other', 
    default=True,
    help="If must-be-git is set (default), the references supplied must be references to git repos. Otherwise (--can-be-other), file paths can be used as references.",
    )
@click.option(
    '--local-scheduler/--global-scheduler', 
    default=False,
    help="If global-scheduler is set (default), the global scheduler will be used. If local scheduler is set, a local scheduler will be used (beware of conflicting runs)."
    )
@click.option(
    '--mock',
    is_flag=True,
    help="""
    Don't actually run the task, but pretend to run it and mark it as done (Does not run dependencies).
    Useful for tasks that have been run outside of trustpipe.
    """
    )
@click.option(
    '--storage-override',
    type=str,
    required=False,
    help="""
    Location to use as output directory of REF. Will fail if multiple refs are supplied, and will not apply to dependencies.
    Useful in combination with --mock to make existing data part of trustpipe.
    """,
)
def entry_point_run(ref: list[str], workers: Optional[int], must_be_git: bool, local_scheduler: bool, mock: bool, storage_override: Optional[str]):
    """Run REF(s).
    
    REF is a github reference of the form: git@github.com:REPO.git#BRANCH:PATH (or a local path for testing purposes).
    """
    
    # if storage-override we can only have one ref.
    assert (storage_override is None) or len(ref) == 1, "We can only mock one ref if path is given"
    _workers = len(ref) if workers is None else workers
    TaskType = MockTask if mock else RunTask
    luigi.build([TaskType(ref=r, must_be_git=must_be_git, storage_override=storage_override) for r in ref], workers=_workers, local_scheduler=local_scheduler)

#####################
# CLI: ls
#####################

class JQFilter(object):
    def __init__(self, jq_filter, **kwargs):
        self.filter = jq.compile(jq_filter, **kwargs)

    def __call__(self, entries):
        for entry in entries:
            match self.filter.input_text(entry.content).all():
                case [True]:
                    yield entry
                case [False]:
                    pass
                case x:
                    xstr = str(x)
                    if len(xstr) > 30:
                        xstr = xstr[:27] + '...'
                    raise ValueError(f"Result of jq filter must be a single boolean, not: {xstr}")

    @classmethod
    def make(cls, kind, selector, value):
        match kind:
            case 'eq':
                return cls(f'{selector}==$value', args={'value': value})
            case 're':
                return cls(f'{selector} | test($value)', args={'value': value})
            case _:
                raise ValueError(f'kind must be one of "eq", "re", not {kind}')

@dataclass
class Entry:
    path: str
    content: str

    @classmethod
    def from_de(cls, de):
        path = de.path
        with open(de, 'rt') as handle:
            content = handle.read()
        return cls(str((pathlib.Path() / path).absolute()), content)

@main.group(name='ls', chain=True, invoke_without_command=True)
@click.option(
        '--data/--repo', 
        default=True,
        help='List metadata about DataTasks (data) or PullTask (repo)'
        )
@click.option(
        '--done', 'status', 
        flag_value='.done', default=True, 
        help='Only list items with status DONE'
        )
@click.option(
        '--failed', 'status', 
        flag_value='.done | not',
        help='Only list items with status FAILED'
        )
@click.option(
        '--all', 'status', 
        flag_value='true',
        help='List all items'
        )
@click.option(
        '--meta/--storage',
        default=True,
        help='Show metadata location (default) or show storage location'
        )
def cli_ls(data, status, meta):
    """List data task in catalog. Use subcommands to filter results"""
    pass

@cli_ls.result_callback()
def ls_pipeline(processors, data, status, meta):
    if data:
        ROOT = DataTarget.catalog_root()
    else:
        ROOT = RepoTarget.catalog_root()

    entries = map(Entry.from_de, os.scandir(ROOT))

    for processor in (JQFilter(status), *processors):
        entries = processor(entries)

    entries = list(entries)

    for entry in entries:
        if meta:
            click.echo(entry.path)
        else:
            click.echo(json.loads(entry.content)['storage'])

@cli_ls.command('filter')
@click.argument('jq_filter')
def cli_ls_jq(jq_filter: str):
    """Filter based on JQ_FILTER (see jq manual for info)"""
    return JQFilter(jq_filter)

@cli_ls.command('ref')
@click.argument('ref')
@click.option('--regex/--exact', default=False, help='use exact matching or regex')
def cli_ls_ref(ref: str, regex: bool):
    """Filter based on task REF"""
    typ = 're' if regex else 'eq'
    return JQFilter.make(typ, '.args.ref', ref)

@cli_ls.command('kind')
@click.argument('kind')
@click.option('--regex/--exact', default=False, help='use exact matching or regex')
def cli_ls_kind(kind: str, regex: bool):
    """Filter based on task KIND"""
    typ = 're' if regex else 'eq'
    return JQFilter.make(typ, '.spec.kind', kind)

@cli_ls.command('name')
@click.argument('name')
@click.option('--regex/--exact', default=False, help='use exact matching or regex')
def cli_ls_name(name: str, regex: bool):
    """Filter based on task NAME"""
    typ = 're' if regex else 'eq'
    return JQFilter.make(typ, '.spec.name', name)
