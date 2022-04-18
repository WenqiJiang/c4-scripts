import json
import jsonlines
import time
import pickle
import os

from bloomfilter import BloomFilter

### TODO: add in bloom filter;
# add out bloom filter (savee to local; to GCP)
# add bloom filter file name list
# add output psg ID
# add primary filter entry
#
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--file_dir_in', type=str, default=0, help="")
parser.add_argument('--file_dir_out', type=str, default=0, help="")
parser.add_argument('--fname', type=str, default=0, help="")
parser.add_argument('--bloomf_local_dir', type=str, default=0, help="local bloom filter obj (pkl file) directory")
parser.add_argument('--bloomf_gcp_dir', type=str, default=0, help="save to GCP the bloom filter obj (pkl file)")
parser.add_argument('--delete_local', type=int, default=1, help="1->delete converted json locally after push it to google storage")

args = parser.parse_args()
file_dir_in = args.file_dir_in
file_dir_out = args.file_dir_out
fname = args.fname
bloomf_local_dir = args.bloomf_local_dir
bloomf_gcp_dir = args.bloomf_gcp_dir
delete_local = args.delete_local

NUM_TOTAL_ITEMS = 1e10 # for the 10 B passagge dataset
FALSE_POS_RATE = 0.001 # false positive probability

if not bloomf_local_dir:
    print("instantiating the bloom filter...")
    bloomf = BloomFilter(NUM_TOTAL_ITEMS, FALSE_POS_RATE)
    print("Size of bit array:{} == {} GB".format(bloomf.size))
    print("False positive Probability:{}".format(bloomf.fp_prob))
    print("Number of hash functions:{}".format(bloomf.hash_count))
else:
    with open(bloomf_local_dir, 'rb') as f:
        bloomf = pickle.load(f)


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

# Save bloom filter object
with open(bloomf_local_dir, 'wb') as f:
    pickle.dump(bloomf, f, protocol=4)

