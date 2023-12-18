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
[data_storage]
root=/data/trustpipe/data # <- this is the directory where data will end up

[meta_storage]
root=/data/trustpipe/meta # <- this is the directory where images and metadata for runs will end up
```

## Trying it out

Ingestion scripts are run via luigi:
```
# start the luigi demon
cd /path/to/trustpipe
luigid

# Run a specific ingestion -> process workflow (in this case, the one at github.com/apsod/litbank.git)
luigi --module trustpipe.tasks ProcessTask --name litb --branch small  --repo apsod/litbank.git 
```

This will start a job that

1. pulls the ingest-(sub)repo, and puts it at `/data/trustpipe/meta/litb/ingest/pull/`
2. pulls the process-(sub)repo, and puts it at `/data/trustpipe/meta/litb/process/pull/`
3. Builds and runs the ingest image, putting data in `/data/trustpipe/data/litb/ingest/...`
4. Builds and runs the process image, putting data in `/data/trustpipe/data/litb/process/...`

It is up to the ingest and process scripts to manage reentrancy.
