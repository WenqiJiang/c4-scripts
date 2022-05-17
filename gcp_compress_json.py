"""
This file is for processing the UW release of C4.
   the input file list is the "ls" result of the multilingual C4 dataseet
The script must be copy to the c4 directory, otherwise git lfs pull will give nothing

Given a list of input file paths (in json / jsonl format),
  generate output files in either sentences or passages
  input: local file
  output: gcs
  
Example:
  python gcp_compress_json.py --file_path_in_list_path ./c4_multilingual_list  --gs_in_dir 'gs://c4multilingual201918/all_multilingual_url/' --gs_out_dir 'gs://c4multilingual201918/all_multilingual_url_compressed/' --skip_exist 1
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
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--file_path_in_list_path', type=str, default=0, help="a list of file name in xxx.json.gz (e.g., xxx.json or xxx.jsonl), stored as plain txt, separated by lines")
parser.add_argument('--gs_in_dir', type=str, default=0, help="e.g., 'gs://c4multilingual201918/all_multilingual_url/'")
parser.add_argument('--gs_out_dir', type=str, default=0, help="e.g., 'gs://c4multilingual201918/all_multilingual_url_compressed/'")
parser.add_argument('--skip_exist', type=int, default=1, help="skip the files that is already inserted into the bloom filter")

args = parser.parse_args()
file_path_in_list_path = args.file_path_in_list_path
gs_in_dir = args.gs_in_dir
gs_out_dir = args.gs_out_dir
skip_exist = args.skip_exist

# multiprocessing cannot access global variable
# finished_count = 0
total_file_count = None

def compress(args):

    path_in, gs_in_dir, gs_out_dir = args # path = dir + fname
    path_in_decompressed = path_in[:-3] # remove '.gz'
    full_path_in = os.path.join(gs_in_dir, path_in_decompressed)
    full_path_out = os.path.join(gs_out_dir, path_in)
    if skip_exist:
        if os.system('gsutil ls {}'.format(full_path_out)) == 0:
            print("File {} already exists, skip...".format(path_in_decompressed), file = sys.stdout)
            return

    # download and compress
    os.system('gsutil cp {} .'.format(full_path_in))
    os.system('gzip {}'.format(path_in_decompressed))


    # cp to google storage
    cmd = 'mv'
    os.system("gsutil {} {} {}".format(cmd, path_in, full_path_out))
    os.system("rm {}".format(path_in))
    os.system("rm {}".format(path_in_decompressed))
    
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

    arg_list = [(file_path_in_list[i], gs_in_dir, gs_out_dir) for i in range(len(file_path_in_list))]
    with multiprocessing.Pool(processes=max_threads) as pool:
        pool.map(compress, arg_list)
                    
    end = time.time()
    t_all = end - start

    print("Total time elapsed: {} sec = {} min = {} hour".format(t_all, t_all / 60, t_all / 3600))
