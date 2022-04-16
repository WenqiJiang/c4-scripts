import json
import jsonlines
import time
import os

from bloomfilter import BloomFilter

n = 1e10 # for the 10 B passagge dataset
p = 0.001 # false positive probability

print("instantiating the bloom filter...")
bloomf = BloomFilter(n,p)
print("Size of bit array:{}".format(bloomf.size))
print("False positive Probability:{}".format(bloomf.fp_prob))
print("Number of hash functions:{}".format(bloomf.hash_count))

fname = 'c4-train.00000-of-02048_passages'
dir_in = '../'
dir_out = '../'

t_start = time.time()
json_lines = None
with open(os.path.join(dir_in, fname + '.jsonl')) as f:
    json_lines = f.readlines()

t_read_json = time.time()

input_line_count = 0
output_line_count = 0

out_json_lines = []
for i, json_line in enumerate(json_lines):
    json_obj = json.loads(json_line)
    passage = json_obj['passage']
    url = json_obj['url']
    
    input_line_count += 1
    if bloomf.add_nonexist(passage):
        out_json_lines.append(json_obj)
        output_line_count += 1

t_add_bloom = time.time()

outfile = os.path.join(dir_out, fname + '_deduplicated.jsonl')
with jsonlines.open(outfile, 'w') as writer:
    writer.write_all(out_json_lines)

t_end = time.time()
t = t_end - t_start
t_bloom = t_add_bloom - t_read_json
print("Time consumption (total): {} sec", t)
print("Time consumption (bloom insert): {} sec", t_bloom)
print("Input lines: {}\tThroughput = {} lines / sec".format(input_line_count, input_line_count / t))
print("Output lines: {}\tThroughput = {} lines / sec".format(output_line_count, output_line_count / t))

