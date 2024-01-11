Luigu/docker workflow to deal with data ingestion. 


## Installation

```
# Requirements: docker, luigi, sqlalchemy
cd /path/to/trustpipe
python setup.py install
```

## Configuration

In `luigi.cfg` you can configure where the images and data is put:

```
[catalog]
# Root folder for metadata and luigi targets
root=/data/trustpipe/catalog

# The following *store* paths are where the orchestrator puts data.
[repostore]
# Root folder for pulled (sub)repos
# Pulltasks puts repos in {PullTask.store}/{taskname}
store=/data/trustpipe/repos

[datastore]
# Root folder for ingestion (where to put ingested data)
# Docker tasks puts data in {DockerTask.store}/{taskname}
store=/data/trustpipe/data
```

## Trying it out

Ingestion scripts are run via luigi:
```
# Run a specific ingestion -> process workflow (in this case, the one at github.com/apsod/litbank.git)
luigi --module trustpipe.tasks DockerTask --repo apsod/litbank.git  --branch small --subpath process
```

This will start a job that

1. Pulls the process-(sub)repo (putting repos in `{PullTask.store} / {task-slug_t45kh45h}`)
2. Identifies that this depends on a separate ingest-(sub)repo
3. Pulls the ingest-(sub)repo
4. Runs the ingest image (putting data in `{DockerTask.store} / {task-slug_t45kh45h}`)
5. Runs the process image

It is up to the ingest and process scripts to manage reentrancy.
