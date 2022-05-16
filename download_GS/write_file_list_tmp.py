file_path_in_list_path = './url_list'

with open(file_path_in_list_path, 'w') as f:
    lines = []
    parent_url = 'https://storage.googleapis.com/c4multilingual201918/tensorflow_datasets/c4_json_url/multilingual201918nodedup/3.0.1/'

    for i in range(2048):
        lines.append(parent_url + 'c4-en.tfrecord-{}-of-02048.json'.format(str(i).zfill(5)) + '\n' )

    for i in range(1024):
        lines.append(parent_url + 'c4-de.tfrecord-{}-of-01024.json'.format(str(i).zfill(5)) + '\n' )

    for i in range(1024):
        lines.append(parent_url + 'c4-ru.tfrecord-{}-of-01024.json'.format(str(i).zfill(5)) + '\n' )

    # either ja or zh are corrupted (TFRecord)
    #for i in range(32):
    #    lines.append(parent_url + 'c4-ja.tfrecord-{}-of-00032.json'.format(str(i).zfill(5)) + '\n' )

    #for i in range(4):
    #    lines.append(parent_url + 'c4-zh.tfrecord-{}-of-00004.json'.format(str(i).zfill(5)) + '\n' )

    f.writelines(lines)

