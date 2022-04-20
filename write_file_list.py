file_path_in_list_path = './bloom_filter_input_file_list'
#file_path_in_list_path = './json_input_file_list'

file_path_out_list_path = './bloom_filter_output_file_list'
#file_path_out_list_path = './passage_output_file_list'

with open(file_path_in_list_path, 'w') as f:
    lines = []
    for i in range(4096):
        # lines.append('/mnt/scratch/wenqi/c4/multilingual/c4-en.tfrecord-{}-of-11264.json'.format(str(i).zfill(5)) + '\n')
        lines.append('/mnt/scratch/wenqi/c4-passages/multilingual/c4-en.tfrecord-{}-of-11264.json'.format(str(i).zfill(5)) + '\n' )
    f.writelines(lines)

with open(file_path_out_list_path, 'w') as f:
    lines = []
    for i in range(4096):
        lines.append('/mnt/scratch/wenqi/c4_deduplicated/multilingual_passages/c4-en.dedup_passages-{}-of-11264.json'.format(str(i).zfill(5)) + '\n' )
        # lines.append('/mnt/scratch/wenqi/c4-passages/multilingual/c4-en.tfrecord-{}-of-11264.json'.format(str(i).zfill(5)) + '\n' )
    f.writelines(lines)
