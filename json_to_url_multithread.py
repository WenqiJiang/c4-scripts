"""
Given a list of input file paths (in json / jsonl format),
  generate output files in either sentences or passages

Example:
  python json_to_url_multithread.py --file_path_in_list_path ./json_input_file_list --file_path_out_list_path passage_output_file_list --skip_exist 1
"""


import os
import sys
import time
import multiprocessing

import json
import jsonlines

from concurrent.futures.thread import ThreadPoolExecutor
from os import listdir

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--file_path_in_list_path', type=str, default=0, help="a list of file name (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--file_path_out_list_path', type=str, default=0, help="a list of file name (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--skip_exist', type=int, default=1, help="skip the files that is already inserted into the bloom filter")

args = parser.parse_args()
file_path_in_list_path = args.file_path_in_list_path
file_path_out_list_path = args.file_path_out_list_path
skip_exist = args.skip_exist

# multiprocessing cannot access global variable
# finished_count = 0
total_file_count = None

def get_urls(args):

    path_in, path_out = args # path = dir + fname
    if skip_exist:
        if os.path.exists(os.path.join(path_out)):
            print("File {} already exists, skip...".format(path_out), file = sys.stdout)
            # finished_count += 1
            # print("Progress: {} out of {} files".format(finished_count, total_file_count), file = sys.stdout)
            return

    # Read json file (1 object per line)
    print("Processing file: {}".format(path_in), file = sys.stdout)
    with open(path_in, 'rb') as f:
        json_lines = f.readlines()

    output_lines = []
    for i, json_line in enumerate(json_lines):

        json_obj = json.loads(json_line)
        url = json_obj['url']
    
        output_lines += [{'url': url}]

    path_out_dir = os.path.dirname(path_out)
    if not os.path.exists(path_out_dir):
        os.makedirs(path_out_dir, exist_ok=True) # mkdir -p 
    with jsonlines.open(path_out, 'w') as writer:
        writer.write_all(output_lines)

    print("Finished processing file: {}".format(path_out), file = sys.stdout)
    # finished_count += 1
    # print("Progress: {} out of {} files".format(finished_count, total_file_count), file = sys.stdout)


if __name__ == '__main__':
    
    start = time.time()
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

    """ Processing with Multi-threading """
    max_threads = multiprocessing.cpu_count() - 1
    if max_threads <= 0:
        max_threads = 1
    # max_threads = 4
    print("Total CPU Cores: {}\tSetting the max workers as {}.".format(multiprocessing.cpu_count(), max_threads))

    arg_list = [(file_path_in_list[i], file_path_out_list[i]) for i in range(len(file_path_in_list))]

    with multiprocessing.Pool(processes=max_threads) as pool:
        pool.map(get_urls, arg_list)
                    
    end = time.time()
    t_all = end - start

    print("Total time elapsed: {} sec = {} min = {} hour".format(t_all, t_all / 60, t_all / 3600))
