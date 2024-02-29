FROM python:3.8-alpine
RUN apk add --no-cache pandoc
RUN pip install pypandoc
VOLUME /input
VOLUME /output
COPY process.py .
ENTRYPOINT ["python", "process.py", "/input", "/output/gutenberg.gz"]
