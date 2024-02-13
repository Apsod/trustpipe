FROM python:3.8-alpine
RUN apk add --no-cache pandoc
RUN apk add --no-cache jq
RUN apk add --no-cache parallel
RUN pip install pypandoc
VOLUME /input
VOLUME /output
COPY process.sh .
COPY process.py .
ENTRYPOINT ["/process.sh"]
