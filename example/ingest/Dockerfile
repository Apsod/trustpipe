FROM alpine:3.18.5
RUN apk add --no-cache wget
COPY urls.txt .
ENTRYPOINT [\
    "wget",\
    "--continue",\
    "--no-verbose",\
    "--directory-prefix=/output",\
    "--input-file=/sources.txt"]
