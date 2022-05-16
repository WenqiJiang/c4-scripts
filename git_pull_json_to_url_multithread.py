"""
The script must be copy to the c4 directory, otherwise git lfs pull will give nothing

Given a list of input file paths (in json / jsonl format),
  generate output files in either sentences or passages
  input: local file
  output: gcs
  
Example:
  python git_pull_json_to_url_multithread.py --file_path_in_list_path ./c4_multilingual_list --gs_out_dir 'gs://c4multilingual201918/all_multilingual_url/' --skip_exist 1
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
parser.add_argument('--file_path_in_list_path', type=str, default=0, help="a list of file name in xxx.json.gz (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--gs_out_dir', type=str, default=0, help="e.g., 'gs://c4multilingual201918/all_multilingual_url/'")
parser.add_argument('--skip_exist', type=int, default=1, help="skip the files that is already inserted into the bloom filter")

args = parser.parse_args()
file_path_in_list_path = args.file_path_in_list_path
gs_out_dir = args.gs_out_dir
skip_exist = args.skip_exist

# multiprocessing cannot access global variable
# finished_count = 0
total_file_count = None

def get_urls(args):

    path_in, gs_out_dir = args # path = dir + fname
    path_in_decompressed = path_in[:-3] # remove '.gz'
    path_out = os.path.join(gs_out_dir, path_in_decompressed)
    if skip_exist:
        if os.system('gsutil ls {}'.format(path_out)) == 0:
            print("File {} already exists, skip...".format(path_in_decompressed), file = sys.stdout)
            return

    # download and decompress
    if not os.path.exists(os.path.join('multilingual/', path_in_decompressed)):
        os.system('git lfs pull --include "{}"'.format(os.path.join('multilingual/', path_in)))
        os.system('gunzip {}'.format(os.path.join('multilingual/', path_in)))

    # Read json file (1 object per line)
    print("Processing file: {}".format(path_in_decompressed), file = sys.stdout)
    with open(os.path.join('multilingual/', path_in_decompressed), 'rb') as f:
        json_lines = f.readlines()

    output_lines = []
    for i, json_line in enumerate(json_lines):

        json_obj = json.loads(json_line)
        url = json_obj['url']
    
        output_lines += [{'url': url}]

    # write to local
    tmp_path = os.path.join('/tmp', path_in_decompressed)
    with jsonlines.open(tmp_path, 'w') as f:
        f.write_all(output_lines)

    # cp to google storage
    cmd = 'mv'
    os.system("gsutil {} {} {}".format(cmd, tmp_path, os.path.join(gs_out_dir, path_in_decompressed)))
    os.system("rm {}".format(os.path.join('multilingual/', path_in_decompressed)))
    os.system("rm {}".format(os.path.join('multilingual/', path_in)))
    
    print("Finished processing file: {}".format(path_in), file = sys.stdout)



if __name__ == '__main__':
    
    start = time.time()
    """ Get the file names """
    with open(file_path_in_list_path, 'r') as f:
        file_path_in_list_uncleaned = f.readlines()
    file_path_in_list = []
    for item in file_path_in_list_uncleaned:
        if item != '' and item != '\n':
            file_path_in_list.append(item.replace('\n', ''))

    """ Processing with Multi-threading """
    max_threads = multiprocessing.cpu_count() - 1
    if max_threads <= 0:
        max_threads = 1
    # max_threads = 4
    print("Total CPU Cores: {}\tSetting the max workers as {}.".format(multiprocessing.cpu_count(), max_threads))

    arg_list = [(file_path_in_list[i], gs_out_dir) for i in range(len(file_path_in_list))]

    with multiprocessing.Pool(processes=max_threads) as pool:
        pool.map(get_urls, arg_list)
                    
    end = time.time()
    t_all = end - start

    print("Total time elapsed: {} sec = {} min = {} hour".format(t_all, t_all / 60, t_all / 3600))
