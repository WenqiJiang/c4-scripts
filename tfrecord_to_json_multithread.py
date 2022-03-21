# Example Usage: 
#   python3 tfrecord_to_json_multithread.py --dir_in 'gs://c4_dataset/tensorflow_datasets/c4/enweb/3.0.1/' --dir_out 'gs://c4_dataset/tensorflow_datasets/c4_json/enweb/3.0.1/'
# it takes ~6 min on a single thread to convert 800MB tfrecord to a same size json in a single thread, and consumes the same time on 10-thread-10-file experiment

import argparse 
import json
import os
import sys
import time
import multiprocessing
import tensorflow as tf

from concurrent.futures.thread import ThreadPoolExecutor
from os import listdir
from nltk.tokenize import sent_tokenize

parser = argparse.ArgumentParser()
parser.add_argument('--dir_in', type=str, default='gs://c4_dataset/tensorflow_datasets/c4/enweb/3.0.1/', help="google storage input directory (tf record)")
parser.add_argument('--dir_out', type=str, default='gs://c4_dataset/tensorflow_datasets/c4_json/enweb/3.0.1/', help="google storage output directory (json)")

args = parser.parse_args()

dir_in = args.dir_in
dir_out = args.dir_out

# multiprocessing cannot access global variable
# finished_count = 0
# total_file_count = None
    
def tfrecord_to_json(args):

    fname, dir_in, dir_out, overwrite, delete_local = args
    
    print("processing {}".format(fname))
    if not overwrite:
        # 0 means return success; other codes means exception and the file doesn't exist
        if os.system('gsutil ls {}'.format(os.path.join(dir_out, fname + '.json'))) == 0:
            print("File {} already exists, skip...".format(fname), file = sys.stdout)
            return
        
    raw_dataset = tf.data.TFRecordDataset([os.path.join(dir_in, fname)])

    feature_description = {
        'text': tf.io.FixedLenFeature([], tf.string, default_value=''),
        'url': tf.io.FixedLenFeature([], tf.string, default_value=''),
        'content-type': tf.io.FixedLenFeature([], tf.string, default_value=''),
        'content-length': tf.io.FixedLenFeature([], tf.string, default_value=''),
        'timestamp': tf.io.FixedLenFeature([], tf.string, default_value='')}

    def _parse_function(example_proto):
        # Parse the input `tf.train.Example` proto using the dictionary above.
        return tf.io.parse_single_example(example_proto, feature_description)

    json_strings = []
    #for raw_record in raw_dataset.take(10):
    for raw_record in raw_dataset:
        record = _parse_function(raw_record)
        # convert to txt
        # https://stackoverflow.com/questions/42144915/convert-tensorflow-string-to-python-string
        text = record['text'].numpy()
        #print(type(text))
        #print(text.decode("utf-8"))
        #print(type(text.decode("utf-8")))
        json_string = json.dumps({'text': record['text'].numpy().decode("utf-8"), 'url': record['url'].numpy().decode("utf-8"), 'timestamp': record['timestamp'].numpy().decode("utf-8") })
        json_strings.append(json_string + '\n')

    # write to local
    tmp_path = os.path.join('/tmp', fname + '.json')
    with open(tmp_path, 'w') as f:
        f.writelines(json_strings)

    # cp to google storage
    if delete_local:
        cmd = 'mv'
    else:
        cmd = 'cp'
    os.system("gsutil {} {} {}".format(cmd, tmp_path, os.path.join(dir_out, fname + '.json')))
    
    print("Finished processing file: {}".format(fname), file = sys.stdout)
    
if __name__ == '__main__':
    
    start = time.time()
    # delete local file once they are copied to google storage
    delete_local=True
    
    # if the output json already exists, overwrite it or skip
    overwrite = False

    # get file list
    flist_txt = os.popen('gsutil ls {}'.format(dir_in)).read()
    file_list = flist_txt.split('\n')
    # e.g., [..., 'gs://c4_dataset/tensorflow_datasets/c4/enweb/3.0.1/c4-train.tfrecord-02047-of-02048', 
    #  'gs://c4_dataset/tensorflow_datasets/c4/enweb/3.0.1/c4-validation.tfrecord-00000-of-00016', ...]
    file_list = sorted([fname for fname in file_list if 'train' in fname or 'validation' in fname])
    # keep file name only, remove dir
    # e.g., [..., 'c4-train.tfrecord-02047-of-02048', 'c4-validation.tfrecord-00000-of-00016', ...]
    file_list = [f.split('/')[-1] for f in file_list] 
   
    print("Number of files: ", len(file_list))
    print("First 5 files:\n", file_list[:5])
    total_file_count = len(file_list)

    max_threads = multiprocessing.cpu_count() - 1
    if max_threads <= 0:
        max_threads = 1
    # max_threads = 4
    print("Total CPU Cores: {}\tSetting the max workers as {}.".format(multiprocessing.cpu_count(), max_threads))

    arg_list = [(file_list[i], dir_in, dir_out, overwrite, delete_local) for i in range(len(file_list))]
    with multiprocessing.Pool(processes=max_threads) as pool:
        pool.map(tfrecord_to_json, arg_list)
    
    end = time.time()
    t_all = end - start
    print("Total time elapsed: {} sec = {} min = {} hour".format(t_all, t_all / 60, t_all / 3600))
