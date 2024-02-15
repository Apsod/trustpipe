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
trustpipe run git@github.com:apsod/trustpipe.git#main:example/process --local-scheduler
```

This will start a job that downloads some books from project gutenberg and converts them to plain text using pandoc, putting data in `path/to/trustpipe/conf/test/data/...`

## What it does

Trustpipe manages task orchestration and configuration. It helps with running tasks that downloads, processes or transforms data and makes sure that they are run in the correct order, logs how/when they are ran, and manages docker volume binding.
It assumes that **tasks** are containerized software defined assets. These **tasks** need two things:

- A dockerfile and build context.
- A specification with **task dependencies** and metadata.

All of this is assumed to reside on a subpath of a git repo.
This repo contains an example directory, containing two such tasks. One `ingest` task and one `process` task:

```
├── ingest
│   ├── Dockerfile
│   ├── urls.txt
│   └── spec.yaml
└── process
    ├── Dockerfile
    ├── process.sh
    ├── process.py
    └── spec.yaml
```

The ingest task is defined by the Dockerfile, and looks like this: 

```
FROM alpine:3.18.5
RUN apk add --no-cache wget
COPY urls.txt .
ENTRYPOINT [\
    "wget",\
    "--continue",\
    "--no-verbose",\
    "--directory-prefix=/output",\
    "--input-file=/urls.txt"]
```

i.e. it wgets a bunch of urls that can be found in urls.txt, and puts them in `/output`.
The specification specifies the name and kind of the task (metadata).

```
name: gutenberg
kind: ingest
```

If we run
```
trustpipe run git@github.com:apsod/trustpipe.git#main:example/ingest
```
Trustpipe does the following: 

- Clones the specified subpath of the repo.
- Builds the docker image.
- Runs the image, mounting a host-side datastore path for this **ref** to /output.

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
COPY process.py .
ENTRYPOINT ["/process.sh"]
```

where `process.py` uses pandoc to convert epub to plain text and wrap it as a json document, and `process.sh` uses `parallel` to run `process.py` over all epub files in the `/input` folder.

```
name: litteraturbanken
kind: process
depends_on:
  input: 
    ref: git@ighub.com:apsod/trustpipe.git#main:example/ingest
```

Here, we specify that this task depends on another task (specified by a ref), namely the above ingest task. We also specify that the local **name** of this task is `input`.

When we run

```
trustpipe run git@github.com:apsod/trustpipe.git#main:example/process
```

Trustpipe does the following:

- Pulls the process-subrepo 
- Identifies that this depends on a separate ingest-subrepo
- Pulls the ingest-subrepo
- Builds an runs the ingest task.
- When/if the ingest task has finished (without errors), it runs the process task.

Trustpipe handles mounting of directories so that the output folders of required tasks are mounted into the corresponding input folders of the running task.

The scheduler ensures that dependencies are run in order, and only once, regardless of how many tasks depend on them. A task can also have several dependencies.
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

will mount the output path of `some_repo_A` to `/dataA`, and `some_repo_B` to `/dataB`.

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
  --mock                          Don't actually run the task, but pretend to
                                  run it and mark it as done (Does not run
                                  dependencies). Useful for tasks that have
                                  been run outside of trustpipe.
  --storage-override TEXT         Location to use as output directory of REF.
                                  Will fail if multiple refs are supplied, and
                                  will not apply to dependencies. Useful in
                                  combination with --mock to make existing
                                  data part of trustpipe.
  --help                          Show this message and exit.
```

### List Completed Tasks
A list of completed tasks can be printed with
`trustpipe list`. The output can be filtered using optional flags. 

```
Usage: trustpipe ls [OPTIONS] COMMAND1 [ARGS]... [COMMAND2 [ARGS]...]...

  List data task in catalog. Use subcommands to filter results

Options:
  --data / --repo     List metadata about DataTasks (data) or PullTask (repo)
  --done              Only list items with status DONE
  --failed            Only list items with status FAILED
  --all               List all items
  --meta / --storage  Show metadata location (default) or show storage
                      location
  --help              Show this message and exit.

Commands:
  filter  Filter based on JQ_FILTER (see jq manual for info)
  kind    Filter based on task KIND
  name    Filter based on task NAME
  ref     Filter based on task REF
```
