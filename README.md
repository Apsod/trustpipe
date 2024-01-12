Luigu/docker workflow to deal with data ingestion. 


## Installation

To install, 

```
git clone git@github.com:apsod/trustpipe.git
cd trustpipe
pip install -e .
```

## Configuration

In `luigi.cfg` you can configure where the images and data is put:

```
[catalog]
# Root folder for metadata and luigi targets
root=/data/trustpipe/catalog

# The following *store* paths are where the orchestrator puts data.
[repostore]
# Root folder where (sub)repos are put
store=/data/trustpipe/repos

[datastore]
# Root folder where data is put
store=/data/trustpipe/data
```

Luigi looks for cfg files at `/etc/luigi/luigi.cfg`, put the config there for global.

## Trying it out

Ingestion scripts are run via luigi:
```
# Run a specific ingestion -> process workflow (in this case, the one at github.com/apsod/litbank.git)
luigi --module trustpipe.tasks DockerTask --repo apsod/litbank.git  --branch small --subpath process
```

This will start a job that

1. Pulls the process-subrepo 
2. Identifies that this depends on a separate ingest-(sub)repo
3. Pulls the ingest-subrepo
4. Runs the ingest image
5. Runs the process image

It is up to the ingest and process scripts to manage reentrancy.
