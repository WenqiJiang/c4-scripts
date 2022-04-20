"""
Given a list of jsonl files from local / GCP (in/out should be the same source), 
 (1) generate the output jsonl files filtering out existed passages
 (2) save the bloom filter 

Example Usage:

  Local files:

  python deduplicate_by_bloom_filter.py --from_gs 0 \
    --file_path_in_list_path bloom_filter_input_file_list \
    --file_path_out_list_path bloom_filter_output_file_list \
    --bloomf_local_path ./BloomFilterPassage.pkl \
    --skip_exist 1 \
    --dedup_key text

  Google Storage files:

  python deduplicate_by_bloom_filter.py --from_gs 1 \
    --file_path_in_list_path bloom_filter_input_file_list \
    --file_path_out_list_path bloom_filter_output_file_list \
    --bloomf_local_path ./BloomFilterPassage.pkl \
    --bloomf_gcp_path gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/BloomFilterPassage.pkl \
    --skip_exist 1 \
    --dedup_key text

For the file path list, an example for local paths (use absolute path):
        /mnt/scratch/wenqi/c4/multilingual/c4-en.tfrecord-00000-of-11264.json
        /mnt/scratch/wenqi/c4/multilingual/c4-en.tfrecord-00001-of-11264.json
    an example for gs path:
        gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/c4-train.tfrecord-00000-of-02048.jsonl
        gs://c4-1billion/tensorflow_datasets/c4/enweb201930/3.0.1/c4-train.tfrecord-00001-of-02048.jsonl
"""

import json
import jsonlines
import time
import pickle
import sys
import os

from bloomfilter import BloomFilter

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--from_gs', type=int, default=0, help="1 -> in/out file are from Google Storage, bloomfilter save to GS; 0 -> from local")
parser.add_argument('--file_path_in_list_path', type=str, default=0, help="a list of file name (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--file_path_out_list_path', type=str, default=0, help="a list of file name (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--bloomf_local_path', type=str, default=0, help="local bloom filter obj (pkl file) directory, end with e.g., bloomf.pkl")
parser.add_argument('--bloomf_gcp_path', type=str, default=0, help="save to GCP the bloom filter obj (pkl file), end with e.g., bloomf.pkl")
parser.add_argument('--skip_exist', type=int, default=1, help="skip the files that is already inserted into the bloom filter")
parser.add_argument('--dedup_key', type=str, default='text', help="the key to deduplicate in json objects, e.g., text, passage, sentence")

args = parser.parse_args()
from_gs = args.from_gs
file_path_in_list_path = args.file_path_in_list_path
file_path_out_list_path = args.file_path_out_list_path
bloomf_local_path = args.bloomf_local_path
bloomf_gcp_path = args.bloomf_gcp_path
skip_exist = args.skip_exist
dedup_key = args.dedup_key


def deduplicate_single_file(bloomf, dedup_key, file_path_in, file_path_out, from_gs):
    """
    Take a single file as input, deduplicate it by key, then write it out
    """
    print("\nDeduplicating ", file_path_in)

    """ Load Input """
    t_start = time.time()
    if from_gs:
        fname = os.path.basename(file_path_in)
        local_path_in = os.path.join('/tmp', fname)
        os.system("gsutil cp {} {}".format(file_path_in, local_path_in))
    else:
        local_path_in = file_path_in

    json_lines = None
    with open(local_path_in, 'rb') as f:
        json_lines = f.readlines()

    t_read_json = time.time()
    print("Finished loading the input")


    """ Add to Bloom Filter """
    input_line_count = 0
    output_line_count = 0

    out_json_lines = []
    for i, json_line in enumerate(json_lines):
        json_obj = json.loads(json_line)
        input_line_count += 1

        if bloomf.add_nonexist(json_obj[dedup_key]):
            out_json_lines.append(json_obj)
            output_line_count += 1

    # There could be duplicated file names in the list, in order to track 
    #   the insertion record
    bloomf.add_to_processed_file_list(file_path_in)

    t_add_bloom = time.time()
    print("Finished insertion to the bloom filter")
    print("Total existed inserted items: {}".format(bloomf.get_item_count()))

    """ Write Output """
    if from_gs:
        fname = os.path.basename(file_path_out)
        local_path_out = os.path.join('/tmp', fname)
    else:
        local_path_out = file_path_out
        local_path_out_dir = os.path.dirname(local_path_out)
        if not os.path.exists(local_path_out_dir):
            os.makedirs(local_path_out_dir, exist_ok=True) # mkdir -p 

    with jsonlines.open(local_path_out, 'w') as writer:
        writer.write_all(out_json_lines)

    if from_gs:
        os.system("gsutil mv {} {}".format(local_path_out, file_path_out))

    t_end = time.time()
    t = t_end - t_start
    t_bloom = t_add_bloom - t_read_json
    print("Finished writing output file")

    print("\n\n")
    print("Time consumption (total): {} sec", t)
    print("Time consumption (bloom insert): {} sec", t_bloom)
    print("Input lines: {}\tThroughput = {} lines / sec".format(input_line_count, input_line_count / t))
    print("Output lines: {}\tThroughput = {} lines / sec".format(output_line_count, output_line_count / t))
    print("Total existed inserted items: {}".format(bloomf.get_item_count()))
    sys.stdout.flush()


if __name__ == '__main__':

    """ Get the file names """
    with open(file_path_in_list_path, 'r') as f:
        file_path_in_list_uncleaned = f.readlines()
    file_path_in_list = []
    for item in file_path_in_list_uncleaned:
        if item != '' and item != '\n':
            file_path_in_list.append(item.replace('\n', ''))

    with open(file_path_out_list_path, 'r') as f:
        file_path_out_list_uncleaned = f.readlines()
    file_path_out_list = []
    for item in file_path_out_list_uncleaned:
        if item != '' and item != '\n':
            file_path_out_list.append(item.replace('\n', ''))

    assert len(file_path_in_list) == len(file_path_out_list), \
        "Input and output file numbers are inconsistent!"


    """ Load Bloom filter """
    if not os.path.exists(bloomf_local_path):
        """ Constants that should remain the same across files """
        NUM_TOTAL_ITEMS = 1e10 # for the 10 B passagge dataset
        FALSE_POS_RATE = 0.001 # false positive probability
        print("WARNING: Bloom filter object path does not exist, please check the "
            "path input if you have the object. Ignore it if you are staring from scratch.")
        print("instantiating a new bloom filter...")
        bloomf = BloomFilter(NUM_TOTAL_ITEMS, FALSE_POS_RATE, dedup_key)
    else:
        print("Loading from existing bloom filter object...")
        with open(bloomf_local_path, 'rb') as f:
            bloomf = pickle.load(f)
        print("Finish loading... Existed inserted items: {}".format(bloomf.get_item_count()))
        assert dedup_key == bloomf.dedup_key, "Inconsistent key for dedupiliation " \
            "in the python argument and the loaded Bloom Filter"

    print("Target total items: {}".format(bloomf.items_count))
    print("Size of bit array:{} == {} GB".format(bloomf.size, bloomf.size/1024/1024/1024/8))
    print("False positive Probability:{}".format(bloomf.fp_prob))
    print("Number of hash functions:{}".format(bloomf.hash_count))
    sys.stdout.flush()


    """ Core Deduplication """
    SAVE_BLOOM_EVERY_N_FILES = 100
    for i in range(len(file_path_in_list)):

        file_path_in = file_path_in_list[i]
        file_path_out = file_path_out_list[i]

        if skip_exist and file_path_in in bloomf.processed_file_list:
            continue
        else:
            deduplicate_single_file(bloomf, dedup_key, file_path_in, file_path_out, from_gs)

            if i % SAVE_BLOOM_EVERY_N_FILES == 0:
                # Save Bloom filter
                with open(bloomf_local_path, 'wb') as f:
                    pickle.dump(bloomf, f, protocol=4)
                if from_gs:
                    os.system("gsutil cp {} {}".format(bloomf_local_path, bloomf_gcp_path))

    """ Save Bloom filter in the end """
    with open(bloomf_local_path, 'wb') as f:
        pickle.dump(bloomf, f, protocol=4)
    if from_gs:
        os.system("gsutil cp {} {}".format(bloomf_local_path, bloomf_gcp_path))

