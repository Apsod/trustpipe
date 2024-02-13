FROM alpine:3.18.5
RUN apk add --no-cache pandoc
RUN apk add --no-cache jq
RUN apk add --no-cache parallel
VOLUME /input
VOLUME /output
COPY process.sh .
ENTRYPOINT ["/process.sh"]
