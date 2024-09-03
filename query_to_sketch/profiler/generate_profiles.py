import os
import copy
import glob
import json
import pickle
import argparse
import numpy as np
from collections import defaultdict

from sketch_control_plane.common.metric_classes import Metric

def file_sort_key(file):
    file_tokens = file.split('/')
    project_name = file_tokens[-6]
    resource_name = file_tokens[-2]
    
    if project_name == 'QuerySketch':
        project_name_tokens = [0, 0]
    else:
        tokens = project_name.split('_')
        project_name_tokens = [int(tokens[0]), int(tokens[1])]

    tokens = resource_name.split('_')
    row = int(tokens[1])
    width = int(tokens[3])
    seed = int(tokens[11])
    resource_tokens = [row, width, seed]

    ret = file_tokens[:1] + project_name_tokens + file_tokens[2:-2] + resource_tokens + file_tokens[-1:]
    return tuple(ret)

def get_counter_size(algo):
    counter_size = 4
    if algo == 'hll' or algo == 'll':
        # int8
        counter_size = 1
    elif algo == 'lc' or algo == 'mrb':
        # 1 bit
        counter_size = 1/8
    return counter_size

# The original width is the number of counter, so it needs to multiply the size of the counter
def convert_width_to_real_byte(algo, widths):
    np_arr = np.array(widths)
    counter_size = get_counter_size(algo)
    return (np_arr * counter_size).astype(np.int32).tolist()

def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    NUM_EPOCHS = 10

    #profiles = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    #profiles = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))))
    profiles = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))))

    files = glob.glob(os.path.join(args.input_cp_dir, args.experiment_name, '**', 'data.pkl.class'), recursive=True)
    files = [f for f in files if '_count_' in f and '_seed_' in f]
    files = sorted(files, key=file_sort_key)

    print('Number of files:', len(files))

    for f_idx, f in enumerate(files):
        if f_idx % 100 == 0:
            print(f'Processing file {f_idx}/{len(files)}')
        data = pickle.load(open(f, 'rb'))

        tokens = f.split('/')
        resource_config_string = tokens[-2]
        sketch = tokens[-5]

        resource_config_tokens = resource_config_string.split('_')
        row = int(resource_config_tokens[1])
        width = int(resource_config_tokens[3])
        level = int(resource_config_tokens[5])
        #seed = int(resource_config_tokens[11])

        pcap = tokens[-4]

        width = int(width * get_counter_size(sketch))

        for d_idx, d in enumerate(data):
            if d_idx == NUM_EPOCHS:
                break
            for metric in d.keys():
                #profiles[(sketch, level)][metric][row][width].append(d[metric].error)
                #profiles[(sketch, level)][metric][row][width][seed].append(d[metric].error)
                #profiles[(sketch, level)][metric][row][width][d_idx].append(d[metric].error)
                profiles[(sketch, level)][metric][row][width][pcap][d_idx].append(d[metric].error)


    for sketch, level in profiles.keys():
        for metric in profiles[(sketch, level)].keys():
            for row in profiles[(sketch, level)][metric].keys():
                for width in profiles[(sketch, level)][metric][row].keys():
                    for pcap in profiles[(sketch, level)][metric][row][width].keys():
                        for epoch in profiles[(sketch, level)][metric][row][width][pcap].keys():
                            profiles[(sketch, level)][metric][row][width][pcap][epoch] = np.mean(profiles[(sketch, level)][metric][row][width][pcap][epoch])
                    data = copy.deepcopy(profiles[(sketch, level)][metric][row][width])
                    profiles[(sketch, level)][metric][row][width] = []
                    for k,v in data.items():
                        for epoch in v.keys():
                            profiles[(sketch, level)][metric][row][width].append(v[epoch])

    for sketch, level in profiles.keys():
        output_file = os.path.join(args.output_dir, f'{sketch}_level_{level}_result.json')
        with open(output_file, 'w') as f:
            json.dump(profiles[(sketch, level)], f, indent=4, sort_keys=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate profiles')
    parser.add_argument('--input_cp_dir', type=str, required=True, help='Input directory')
    parser.add_argument('--experiment_name', default='QuerySketch')
    parser.add_argument('--output_dir', type=str, required=True, help='Output directory')
    args = parser.parse_args()
    main(args)
