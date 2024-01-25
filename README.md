Luigu/docker workflow to deal with data ingestion. 


## Installation

```
git clone git@github.com:apsod/trustpipe.git
cd trustpipe
pip install -e .
```

Trustpipe uses ssh to clone repos, if you get an error like `permission denied (publickey)`, you have not configured git/github to use ssh, and it will (currently) not work.

## Trying it out


If you just want to try it out, do the following, (preferrably in a conda environment or venv):

```
## INSTALL
git clone git@github.com:apsod/trustpipe.git
cd trustpipe
pip install -e .
## RUN
cd conf
trustpipe run git@github.com:apsod/litbank.git#small:process --local-scheduler
```

This will start a job that downloads some books from litteraturbanken and converts them to plain text using pandoc, putting data in `path/to/trustpipe/conf/test/data/...`

## What it does

Trustpipe manages task orchestration and configuration. It helps with running tasks that downloads, processes or transforms data and makes sure that they are run in the correct order, logs how/when they are ran, and manages docker volumes (containers can be filesystem agnostic).
It assumes that **tasks** are containerized, software defined assets. These **tasks** need two things:

- A dockerfile and build context.
- A specification with **task dependencies** and metadata.

All of this is assumed to reside on a subpath of a git repo.
For example, the repo https://github.com/Apsod/litbank contains two tasks: an `ingest` task and a `process` task, and looks like this: 

```
├── ingest
│   ├── Dockerfile
│   ├── mklist.sh
│   ├── sources.txt
│   └── spec.yaml
└── process
    ├── Dockerfile
    ├── process.sh
    └── spec.yaml
```

The ingest task is defined by the Dockerfile, and looks like this: 

```
FROM alpine:3.18.5
RUN apk add --no-cache wget
COPY sources.txt .
VOLUME /data
ENTRYPOINT [\
    "wget",\
    "--continue",\
    "--no-verbose",\
    "--force-directories",\
    "--no-host-directories",\
    "--cut-dirs=2",\
    "--directory-prefix=/data",\
    "--input-file=sources.txt"]
```

i.e. it wgets a bunch of urls that can be found in sources.txt, and puts them in `/data`.

The specification specifies the name and kind of the task (metadata) and specifies where **in the container** the output data is stored (defaults to `/output`).

```
name: litteraturbanken
kind: ingest
output: /data
```

When we run
```
trustpipe run git@github.com:apsod/litbank.git#small:ingest
```
Trustpipe clones the specified subpath of the repo (and branch/tag), builds the docker image, and runs it, mounting the folder specified by `datastore` to the tasks output. The git reference format is `git@github.com:REPO#BRANCH:PATH`

### Dependencies

The process task is a bit more interesting. The Dockerfile looks like this: 

```
FROM alpine:3.18.5
RUN apk add --no-cache pandoc
RUN apk add --no-cache jq
RUN apk add --no-cache parallel
VOLUME /input
VOLUME /output
COPY process.sh .
ENTRYPOINT ["/process.sh"]
```

where `process.sh` essentially uses pandoc to convert from epub to plain text. However, the specification looks like this: 

```
name: litteraturbanken
kind: process
depends_on:
  input: 
    ref: git@ighub.com:apsod/litbank.git#small:ingest
```

Here, we specify that this task depends on another task (specified by a repo, subpath, and branch), namely the above ingest task. We also specify that the local **name** of this task is `input`.

When we run
```
trustpipe run git@github.com:apsod/litbank.git#small:process
```

Trustpipe does the following:

1. Pulls the process-subrepo 
2. Identifies that this depends on a separate ingest-subrepo
3. Pulls the ingest-subrepo
4. Builds an runs the ingest task. Mounting the host path specified by `datastore` to the internal container path specified by the task specification (default: `/output`)
5. When/if the ingest task has finished (without errors), it runs the process task. Mounting the host path of the ingest task output to `/input` (read-only), and the host path specified by `datastore` to `/output`

Depdencies that have already been run are not rerun, and a task can have several dependencies: 

For example, a task with the following specification:

```
name: test
kind: process
depends_on:
  dataA: 
    ref: git@github.com:some_repo_A
  dataB:
    ref: git@github.com:some_repo_B
```

will mount the output path of `some_repo_A, task_A` to `/dataA`, and `some_repo_B, task_B` to `/dataB`.

The idea is to make it simpler to write ingestion and processing scripts that are as portable as possible, and that are agnostic to the underlying filesystem. 

## Configuration

In `luigi.cfg` you can configure where the images and data is put:

```
[catalog]
# Root folder for metadata and luigi targets
root=test/catalog

# The following *store* paths are where the orchestrator puts data.
[repostore]
# Folder for repos
# task.repo, task.subpath, task.branch
# task.slug, task.hash, task.basename (task.slug_task.hash)
store=test/repos/{task.basename}

[datastore]
# Folder for ingestion
# task.repo, task.subpath, task.branch
# task.slug, task.hash, task.basename (task.slug_task.hash)
# spec.name, spec.kind, spec.X ...
store=test/data/{task.basename}
#store=/data/trustpipe/data/{spec.name}/{spec.kind}
```

The default configuration uses relative paths, which are **not recommended** for actual use, but are there for demonstration purposes.
When you have configured trustpipe to your liking, put the config file in `/etc/luigi/luigi.cfg`, or point to it using the environment variable `LUIGI_CONFIG_PATH`.

## Central scheduler

To start the central scheduler, run the luigi demon (in a screen or tmux): `luigid`

The central scheduler makes sure that we don't start several competing runs of the same task. If you want to try it without the central scheduler, simply add `--local-scheduler` to the luigi calls, but beware that this makes it possible that we have several competing runs of the same task.

## Command Line Interface (CLI)

The trustpipe CLI is available via the command `trustpipe` and offers various functionalities.

### Run Tasks
Start runs using the `trustpipe run` command. This will run tasks using luigi. 

```
Usage: trustpipe run [OPTIONS] [REF]...

  Run REF(s).

  REF is a github reference of the form: git@github.com:REPO.git#BRANCH:PATH
  (or a local path for testing purposes).

Options:
  --workers INTEGER               Numbers of concurrent tasks to run. Defaults
                                  to the number of REFs supplied.
  --must-be-git / --can-be-other  If must-be-git is set (default), the
                                  references supplied must be references to
                                  git repos. Otherwise (--can-be-other), file
                                  paths can be used as references.
  --local-scheduler / --global-scheduler
                                  If global-scheduler is set (default), the
                                  global scheduler will be used. If local
                                  scheduler is set, a local scheduler will be
                                  used (beware of conflicting runs).
  --help                          Show this message and exit.
```

### List Completed Tasks
A list of completed tasks can be printed with
`trustpipe list`. The output can be filtered using optional flags. 

```
$ trustpipe list --help

Usage: trustpipe list [OPTIONS]

  LIST COMPLETED TASKS. USE OPTIONS BELOW TO FILTER JSON FILES.

Options:
  --jq_filter TEXT  filter json files using jq filter, e.g. '.spec.kind =
                    "process"'
  --kind TEXT       filter json files by kind, e.g. process
  --name TEXT       filter json files by name, e.g. litteraturbanken
  --help            Show this message and exit.
```
