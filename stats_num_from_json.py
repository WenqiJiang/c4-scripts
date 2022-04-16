"""
Given a json file of c4 texts, specify the number of 
    (1) web pages (json objects) 
    (2) number of passages (by '\n')
    (3) (optional) number of sentences

Example
    python stats_num_from_json.py --dir_in '/home/contact_ds3lab/' --fname 'c4-train.tfrecord-00000-of-02048.json' 
"""

import json
import os
import numpy as np

from nltk.tokenize import sent_tokenize
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--dir_in', type=str, default=0, help="")
parser.add_argument('--fname', type=str, default=0, help="")

args = parser.parse_args()
dir_in = args.dir_in
fname = args.fname

stat_sentences = True

lines = []

# Read json file (1 object per line)
json_lines = None
with open(os.path.join(dir_in, fname)) as f:
    json_lines = f.readlines()

num_pages = 0
num_passages = 0
words_per_psg = []
if stat_sentences:
    num_sentences = 0
for i, json_line in enumerate(json_lines):
    #if i >= 10000:
    #    break
    page = json.loads(json_line)['text']
    passages = page.split('\n')
    for psg in passages:
        words_per_psg.append(len(psg.split(" ")))
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

words_per_psg.sort()
total_words_in_psg = np.sum(words_per_psg)
average_words_in_psg = total_words_in_psg / num_passages
print("Passage stats: ")
print("\tAverage length: {}\tMedian length: {}".format(average_words_in_psg, words_per_psg[int(num_passages/2)]))
print("\tMin length: {}\tMax length: {}".format(words_per_psg[0], words_per_psg[-1]))
