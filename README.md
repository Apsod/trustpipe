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
[PullTask]
# Root folder for pulled (sub)repos
# Pulltasks puts repos in {PullTask.store}/{repo}/{branch}/{subpath}
store=/data/trustpipe/repos

[IngestTask]
# Root folder for ingestion (where to put ingested data)
# Ingest-dockers puts data in {IngestTask.store}/{name}
store=/data/trustpipe/data/ingested

[ProcessTask]
# Root folder for processing (where to put processed data)
# Process-dockers puts data in {ProcessTask.store}/{name}
store=/data/trustpipe/data/processed
```

## Trying it out

Ingestion scripts are run via luigi:
```
# Run a specific ingestion -> process workflow (in this case, the one at github.com/apsod/litbank.git)
luigi --module trustpipe.tasks ProcessTask --name litb --branch small  --repo apsod/litbank.git 
```

This will start a job that

1. pulls and saves the ingest-(sub)repo
2. pulls and saves the process-(sub)repo
3. Builds and runs the ingest image, with bind-mount `-v {IngestTask.store}/litb:/data')`
4. Builds and runs the process image, with bind-mounts `-v {IngestTask.store}/litb:/input/ro -v {ProcessTask.store}/litb:/output`

It is up to the ingest and process scripts to manage reentrancy.
