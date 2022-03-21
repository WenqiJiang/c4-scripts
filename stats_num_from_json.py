"""
Given a json file of c4 texts, specify the number of 
    (1) web pages (json objects) 
    (2) number of passages (by '\n')
    (3) (optional) number of sentences

"""

import json
import os

from nltk.tokenize import sent_tokenize

stat_sentences = True

lines = []

fname = 'c4-train.00000-of-02048'
dir_in = '/home/contact_ds3lab/'

# Read json file (1 object per line)
json_lines = None
with open(os.path.join(dir_in, fname + '.json')) as f:
    json_lines = f.readlines()

num_pages = 0
num_passages = 0
if stat_sentences:
    num_sentences = 0
for json_line in json_lines:
    page = json.loads(json_line)['text']
    passages = page.split('\n')
    sentences = sent_tokenize(page)
    num_pages += 1
    num_passages += len(passages)
    if stat_sentences:
        num_sentences += len(sentences)

print("File {} statistics:".format(os.path.join(dir_in, fname + '.json')))
print("Page number: ", num_pages)
print("Passage number: ", num_passages)
if stat_sentences:
    print("Sentence number: ", num_sentences)
