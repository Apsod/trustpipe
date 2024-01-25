"""Command Line Interface of trustpipe"""

import os
import click
import jq
import luigi
from typing import Optional, List

from trustpipe.tasks import DataTarget, DockerTask


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
# CLI: RUN
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
def entry_point_run(ref: list[str], workers: Optional[int], must_be_git: bool, local_scheduler: bool):
    """Run REF(s).
    
    REF is a github reference of the form: git@github.com:REPO.git#BRANCH:PATH (or a local path for testing purposes).
    """
    _workers = len(ref) if workers is None else workers
    luigi.build([DockerTask(ref=r, must_be_git=must_be_git) for r in ref], workers=_workers, local_scheduler=local_scheduler)



#################################
# CLI: LIST
#################################

@main.command(name="list")
@click.option(
    "--jq_filter",
    type=str,
    required=False,
    help="filter json files using jq filter, e.g. \'.spec.kind = \"process\"\'",
)
@click.option(
    "--kind",
    type=str,
    required=False,
    help="filter json files by kind, e.g. process",
)
@click.option(
    "--name",
    type=str,
    required=False,
    help="filter json files by name, e.g. litteraturbanken",
)
def entry_point_list(jq_filter: Optional[str], kind: Optional[str], name: Optional[str]) -> None:
    """LIST COMPLETED TASKS. USE OPTIONS BELOW TO FILTER JSON FILES."""
    # parse input arguments and create JsonFilter instance
    jq_filters = []
    if jq_filter is not None:
        jq_filters.append(jq_filter)
    if kind is not None:
        jq_filters.append(f".spec.kind == \"{kind}\"")
    if name is not None:
        jq_filters.append(f".spec.name == \"{name}\"")
    json_filter = JsonFilter(jq_filters) if len(jq_filters) else None

    # apply JsonFilter instance and print list of docs
    entries = os.scandir(DataTarget.catalog_root())
    for de in entries:
        doc = open(de).read()
        if json_filter is None or json_filter.apply(doc):
            print(de.path)
