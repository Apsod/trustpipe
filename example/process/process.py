import re
import json
import itertools
import sys
import os
import gzip
from multiprocessing import Process, Queue

import pypandoc

end_of_prefix = re.compile(r'^\*{3} START OF THE PROJECT GUTENBERG EBOOK .*? \*{3}$')
start_of_suffix = re.compile(r'^\*{3} END OF THE PROJECT GUTENBERG EBOOK .*? \*{3}$')

def process_epub(data):
    doc = pypandoc.convert_text(data, 'plain', format='epub', extra_args=['--wrap=preserve'])
    lines = iter(doc.split('\n'))

    prefix = []
    text = []
    suffix = []
    for line in lines:
        prefix.append(line)
        if end_of_prefix.match(line):
            break

    prefix = '\n'.join(prefix)

    for line in lines:
        if start_of_suffix.match(line):
            suffix.append(line)
            break
        else:
            text.append(line)

    text = '\n'.join(text)
            
    for line in lines:
        suffix.append(line)

    suffix = '\n'.join(suffix)

    return {'prefix': prefix, 'text': text, 'suffix': suffix}

def reader(root, queue):
    for de in os.scandir(root):
        if de.name.endswith('epub.noimages'):
            with open(de, 'rb') as handle:
                data = handle.read()
                if data:
                    queue.put(data)

def worker(in_queue, out_queue):
    while (data := in_queue.get()):
        out_queue.put(json.dumps(process_epub(data)))
    

def writer(dest, in_queue):
    with gzip.open(dest, 'wt') as out:
        while (data := in_queue.get()):
            out.write(data)
            out.write('\n')

if __name__ == '__main__':
    root = sys.argv[1]
    out = sys.argv[2]
    P = max(8, os.cpu_count())

    raw_queue = Queue(P*2)
    formatted_queue = Queue(P*2)
    
    reader_process = Process(target=reader, args=(root, raw_queue))
    worker_processes = [Process(target=worker, args=(raw_queue, formatted_queue)) for _ in range(P)]
    writer_process = Process(target=writer, args=(out, formatted_queue,))

    reader_process.start()
    for worker in worker_processes:
        worker.start()
    writer_process.start()

    # When all has been read
    # Send stop signal to workers
    reader_process.join()
    for _ in worker_processes:
        raw_queue.put(None)
    
    # When all workers are finished
    # Send stop signal to writer
    for worker in worker_processes:
        worker.join()
    formatted_queue.put(None)
    
    # Wait for writer to finish
    writer_process.join()
