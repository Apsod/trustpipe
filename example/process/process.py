import re
import json
import itertools
import sys
import pypandoc

end_of_prefix = re.compile(r'^\*{3} START OF THE PROJECT GUTENBERG EBOOK .*? \*{3}$')
start_of_suffix = re.compile(r'^\*{3} END OF THE PROJECT GUTENBERG EBOOK .*? \*{3}$')

def process_epub(file):
    doc = pypandoc.convert_file(file, 'plain', format='epub', extra_args=['--wrap=preserve'])
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

    return prefix, text, suffix

if __name__ == '__main__':
    prefix, text, suffix = process_epub(sys.argv[1])
    print(json.dumps(
        dict(prefix=prefix, text=text, suffix=suffix)
    ))
