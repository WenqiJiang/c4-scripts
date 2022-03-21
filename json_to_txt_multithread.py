import json
import os
import sys
import multiprocessing

from concurrent.futures.thread import ThreadPoolExecutor
from os import listdir
from nltk.tokenize import sent_tokenize

# multiprocessing cannot access global variable
# finished_count = 0
total_file_count = None

def json_to_txt(args):
    fname, dir_in, dir_out, overwrite = args
    if not overwrite:
        if os.path.exists(os.path.join(dir_out, fname[:-len('.json')] + '.txt')):
            print("File {} already exists, skip...".format(fname), file = sys.stdout)
            # finished_count += 1
            # print("Progress: {} out of {} files".format(finished_count, total_file_count), file = sys.stdout)
            return

    # Read json file (1 object per line)
    print("Processing file: {}".format(fname), file = sys.stdout)
    json_objs = None
    with open(os.path.join(dir_in, fname)) as f:
        json_objs = f.readlines()

    paragraphs = []
    for json_line in json_objs:
        paragraphs.append(json.loads(json_line)['text'])

    lines = []
    for paragraph in paragraphs:
        sentences = sent_tokenize(paragraph)
        sentences = [sent + '\n' for sent in sentences]
        lines += sentences

    # remove the last empty line
    if lines[-1] == '\n':
        lines.pop()

    # Write lines as plain text
    with open(os.path.join(dir_out, fname[:-len('.json')] + '.txt'), 'w') as f:
        f.writelines(lines)

    print("Finished processing file: {}".format(fname), file = sys.stdout)
    # finished_count += 1
    # print("Progress: {} out of {} files".format(finished_count, total_file_count), file = sys.stdout)


if __name__ == '__main__':
    
    overwrite = False
    dir_in = '/mnt/scratch/wenqi/c4/en/'
    dir_out = '../data/plain_c4/en/'
    file_list = listdir(dir_in)
    file_list = sorted([f for f in file_list if f[-len('.json'):] == '.json'])
    print("Number of files: ", len(file_list))
    print("First 5 files:\n", file_list[:5])
    total_file_count = len(file_list)

    max_threads = multiprocessing.cpu_count() - 1
    if max_threads <= 0:
        max_threads = 1
    # max_threads = 4
    print("Total CPU Cores: {}\tSetting the max workers as {}.".format(multiprocessing.cpu_count(), max_threads))

    """
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        tid = 0
        for fname in file_list:
            print("Submitting thread {}".format(tid))
            executor.submit(json_to_txt, fname, dir_in, dir_out)
            tid += 1
    """
    arg_list = [(file_list[i], dir_in, dir_out, overwrite) for i in range(len(file_list))]
    with multiprocessing.Pool(processes=max_threads) as pool:
        pool.map(json_to_txt, arg_list)
