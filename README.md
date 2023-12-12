Luigu/docker workflow to deal with data ingestion. 

## Installation

```
# Requirements: docker, luigi, sqlalchemy
python setup.py install
```

## Configuration

In `luigi.cfg` you can configure where the images and data is put:

```
[storage]
root=/data/trustpipe # <--- this is the root directory for the storage
```

## Trying it out

To run an ingestion script, point to the docker url you want to run and 
```
# start the luigi demon
luigid

# Run an ingestion workflow (point to a github docker url)
luigi IngestTask --name riksdag --path github.com/Apsod/anforanden.git#main:ingest --module trustpipe.ingest
```

This will start a job that

1. Builds a docker image, and puts it at `root/riksdag.image`
2. Runs the docker image, putting data in `root/riksdag/...`
3. When the docker image finishes, it creates a symlink `root/riksdag.done -> root/riksdag/`.

It is up to the ingest script to manage reentrancy.
