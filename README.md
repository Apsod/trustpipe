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

# Run a specific ingestion workflow (point to a github docker url)
luigi IngestTask --name riksdag --path github.com/Apsod/anforanden.git#main:ingest --module trustpipe.ingest
```

This will start a job that

1. Builds a docker image, and puts it at `/data/trustpipe/meta/riksdag/img`
2. Runs the docker image, putting data in `/data/trustpipe/data/riksdag/...`
3. When the docker image finishes, it creates a symlink `/data/trustpipe/meta/riksdag/data -> /data/trustpipe/data/riksdag`.

It is up to the ingest script to manage reentrancy.
