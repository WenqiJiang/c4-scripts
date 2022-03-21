import json
import os

from nltk.tokenize import sent_tokenize

lines = []

fname = 'c4-train.00000-of-01024'
dir_in = '/mnt/scratch/wenqi/c4/en/'
dir_out = '../data/plain_c4/en/'

# Read json file (1 object per line)
json_lines = None
with open(os.path.join(dir_in, fname + '.json')) as f:
    json_lines = f.readlines()

for json_line in json_lines:
    paragraph = json.loads(json_line)['text']
    sentences = sent_tokenize(paragraph)
    sentences = [sent + '\n' for sent in sentences]
    lines += sentences

# remove the last empty line
if lines[-1] == '\n':
    lines.pop()

# Write lines as plain text
with open(os.path.join(dir_out, fname + '.txt'), 'w') as f:
    f.writelines(lines)

