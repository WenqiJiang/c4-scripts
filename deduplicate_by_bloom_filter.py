"""
Given a single jsonl file from local / GCP (in/out should be the same source), 
 (1) generate an output jsonl file filtering out existed passages
 (2) save the bloom filter 

Example Usage:
  python deeduplicate_by_bloom_filter.py --from_gs 0 \
    --file_path_in ../c4-train.00000-of-02048_passages.jsonl \
    --file_path_out ../c4-train.00000-of-02048_passages_deduplicated.jsonl \
    --bloomf_local_path ./BloomFilterPassage.pkl
  python deeduplicate_by_bloom_filter.py --from_gs 1 \
    --file_path_in gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/c4-train.tfrecord-00000-of-02048.jsonl \
    --file_path_out gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/c4-train.tfrecord-00000-of-02048_deduplicated.jsonl \
    --bloomf_local_path ./BloomFilterPassage.pkl
    --bloomf_gcp_path gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/BloomFilterPassage.pkl
"""

import json
import jsonlines
import time
import pickle
import os

from bloomfilter import BloomFilter

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--from_gs', type=int, default=0, help="1 -> in/out file are from Google Storage, bloomfilter save to GS; 0 -> from local")
parser.add_argument('--file_path_in', type=str, default=0, help="with file name, e.g., xxx.json or xxx.jsonl")
parser.add_argument('--file_path_out', type=str, default=0, help="with file name, e.g., xxx.json or xxx.jsonl")
parser.add_argument('--bloomf_local_path', type=str, default=0, help="local bloom filter obj (pkl file) directory, end with e.g., bloomf.pkl")
parser.add_argument('--bloomf_gcp_path', type=str, default=0, help="save to GCP the bloom filter obj (pkl file), end with e.g., bloomf.pkl")

args = parser.parse_args()
from_gs = args.from_gs
file_path_in = args.file_path_in
file_path_out = args.file_path_out
bloomf_local_path = args.bloomf_local_path
bloomf_gcp_path = args.bloomf_gcp_path
delete_local = args.delete_local

DEDUP_KEY = 'passage'

if not os.path.exists(bloomf_local_path):
    """ Constants that should remain the same across files """
    NUM_TOTAL_ITEMS = 1e10 # for the 10 B passagge dataset
    FALSE_POS_RATE = 0.001 # false positive probability
    print("WARNING: Bloom filter object path does not exist, please check the "
        "path input if you the object. Ignore it if you are staring from scratch.")
    print("instantiating a new bloom filter...")
    bloomf = BloomFilter(NUM_TOTAL_ITEMS, FALSE_POS_RATE)
else:
    print("Loading from existing bloom filter object...")
    with open(bloomf_local_path, 'rb') as f:
        bloomf = pickle.load(f)
    print("Finish loading... Existed inserted items: {}".format(bloomf.get_item_count()))

print("Target total items: {}".format(bloomf.items_count))
print("Size of bit array:{} == {} GB".format(bloomf.size))
print("False positive Probability:{}".format(bloomf.fp_prob))
print("Number of hash functions:{}".format(bloomf.hash_count))


""" Load Input """
t_start = time.time()
if from_gs:
    fname = file_path_in.split('/')[-1]
    local_path_in = os.path.join('/tmp', fname)
    os.system("gsutil cp {} {}".format(file_path_in, local_path_in))
else:
    local_path_in = file_path_in

json_lines = None
with open(local_path_in, 'rb') as f:
    json_lines = f.readlines()

t_read_json = time.time()


""" Add to Bloom Filter """
input_line_count = 0
output_line_count = 0

out_json_lines = []
for i, json_line in enumerate(json_lines):
    json_obj = json.loads(json_line)
    input_line_count += 1

    if bloomf.add_nonexist(passage[DEDUP_KEY]):
        out_json_lines.append(json_obj)
        output_line_count += 1

# There could be duplicated file names in the list, in ordeer to track 
#   the insertion record
bloomf.add_to_processed_file_list(file_path_in)

t_add_bloom = time.time()
print("Total existed inserted items: {}".format(bloomf.get_item_count()))

""" Write Output """
if from_gs:
    fname = file_path_out.split('/')[-1]
    local_path_out = os.path.join('/tmp', fname)
else:
    local_path_out = file_path_out

with jsonlines.open(local_path_out, 'w') as writer:
    writer.write_all(out_json_lines)

if from_gs:
    os.system("gsutil mv {} {}".format(local_path_out, file_path_out))

t_end = time.time()
t = t_end - t_start
t_bloom = t_add_bloom - t_read_json
print("Time consumption (total): {} sec", t)
print("Time consumption (bloom insert): {} sec", t_bloom)
print("Input lines: {}\tThroughput = {} lines / sec".format(input_line_count, input_line_count / t))
print("Output lines: {}\tThroughput = {} lines / sec".format(output_line_count, output_line_count / t))

""" Save Bloom filter """
with open(bloomf_local_path, 'wb') as f:
    pickle.dump(bloomf, f, protocol=4)
if from_gs:
    os.system("gsutil cp {} {}".format(bloomf_local_path, bloomf_gcp_path))

