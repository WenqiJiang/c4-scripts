import json
import jsonlines
import os

from nltk.tokenize import sent_tokenize

GEN_PASSAGE = True
GEN_SENTENCE = True

passage_lines = []
sentence_lines = []

fname = 'c4-train.00000-of-02048'

dir_in = '../'
dir_out = '../'

# Read json file (1 object per line)
json_lines = None
with open(os.path.join(dir_in, fname + '.json')) as f:
    json_lines = f.readlines()

for i, json_line in enumerate(json_lines):

    json_obj = json.loads(json_line)
    page = json.loads(json_line)['text']
    url = json.loads(json_line)['url']

    if GEN_PASSAGE:
        passages = page.split('\n')
        passage_lines += [{'passage:': psg, 'url': url} for psg in passages]

    if GEN_SENTENCE: 
        sentences = sent_tokenize(page)
        sentence_lines += [{'sentence:': st, 'url': url} for st in sentences]


passage_file = os.path.join(dir_out, fname + '_passages.jsonl')
with jsonlines.open(passage_file, 'w') as writer:
    writer.write_all(passage_lines)

sentence_file = os.path.join(dir_out, fname + '_sentences.jsonl')
with jsonlines.open(sentence_file, 'w') as writer:
    writer.write_all(sentence_lines)

