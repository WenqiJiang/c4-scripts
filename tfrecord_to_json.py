# it takes ~6 min on a single thread to convert 800MB tfrecord to a same size json

import tensorflow as tf
import json
import os

delete_local=False

dir_in = 'gs://c4_dataset/tensorflow_datasets/c4/enweb/3.0.1/'
dir_out = 'gs://c4_dataset/tensorflow_datasets/c4_json/enweb/3.0.1/'
fname = "c4-train.tfrecord-00000-of-02048"
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
