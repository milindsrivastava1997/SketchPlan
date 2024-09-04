import os
import argparse

from sketch_control_plane.common import univmon_lib
from sketch_control_plane.QuerySketch.select_params.sketch import common
from sw_dp_simulator.file_io.py.read_univmon import load_univmon
from python_lib.run_parallel_helper import ParallelRunHelper

import constants

def get_cs_estimates_map(univmon_result, rows, width, total_levels, dataplane):
    gt_result = univmon_result['gt']
    index_hash_list = univmon_result["index_hash_list"]
    res_hash_list = univmon_result["res_hash_list"]
    sampling_hash_list = univmon_result["sampling_hash_list"]
    sketch_array_list = univmon_result["sketch_array_list"]
    hash_function = constants.DP_HASH_MAP[dataplane]

    result = {l:[] for l in range(total_levels)}

    for idx, gt_element in enumerate(gt_result):
        true_flow_count = gt_element[1]
        flowkey = gt_element[2]

        gt_element_level = univmon_lib.last_level_lib(True, flowkey, hash_function, sampling_hash_list, total_levels)
        est = common.counter_estimate_cs(flowkey, sketch_array_list[gt_element_level], index_hash_list[gt_element_level], res_hash_list[gt_element_level], rows, width, hash_function, True)
        result[gt_element_level].append((est, idx))

    return result

def get_topk_flows(cs_estimates, univmon_result, current_level, topk):
    gt_result = univmon_result['gt']
    sorted_cs_estimates = sorted(cs_estimates, key=lambda item: item[0], reverse=True)
    topk_flows = [(gt_result[idx], est) for (est, idx) in sorted_cs_estimates[:topk]]
    return topk_flows

def dump_topk_flows(output_file, topk_flows):
    with open(output_file, 'w') as fout:
        fout.write('dstIP,dstPort\n')
        for flow, cs_est in topk_flows:
            string_key = flow[0]
            # use estimate from cs, not ground truth
            #estimate = flow[1]
            estimate = cs_est
            estimate = int(estimate)
            flowkey = flow[2]
            flowkey_str = '[' + ' '.join([str(flowkey.src_addr), str(flowkey.src_port), str(flowkey.dst_addr), str(flowkey.dst_port), str(flowkey.proto)]) + ']'
            output_str = ' '.join([string_key, str(estimate), flowkey_str])
            fout.write(output_str + '\n')

def run_parallel_helper_function(input_path, rows, width, total_levels, dataplane):
    univmon_result = load_univmon(input_path, None, width, rows, total_levels, dataplane, load_top200=False)
    cs_estimates_map = get_cs_estimates_map(univmon_result, rows, width, total_levels, dataplane)

    for current_level in range(total_levels):
        output_file = os.path.join(input_path, 'level_' + str(current_level).zfill(2), 'top_200.txt')
        cs_sim_total = univmon_result["sketch_array_list"][current_level]
        cs_estimates = cs_estimates_map[current_level]
        topk_flows = get_topk_flows(cs_estimates, univmon_result, current_level, topk=1000)
        dump_topk_flows(output_file, topk_flows)

def main(args):
    args_list = []
    input_dir_name = 'result_{}_dp'.format(args.dataplane)
    input_root_dir = os.path.join(constants.ROOT_DIR, input_dir_name, args.project_name)

    sketches = sorted(os.listdir(input_root_dir))
    for sketch in sketches:
        sketch_dir = os.path.join(input_root_dir, sketch)
        pcaps = sorted(os.listdir(sketch_dir))
        for pcap in pcaps:
            pcap_dir = os.path.join(sketch_dir, pcap)
            flowkeys = sorted(os.listdir(pcap_dir))
            for flowkey in flowkeys:
                flowkey_dir = os.path.join(pcap_dir, flowkey)
                configs = sorted(os.listdir(flowkey_dir))
                for config in configs:
                    config_dir = os.path.join(flowkey_dir, config)
                    epochs = sorted(os.listdir(config_dir))

                    tokens = config.split('_')
                    rows = int(tokens[1])
                    width = int(tokens[3])
                    levels = int(tokens[5])

                    for epoch in epochs:
                        epoch_dir = os.path.join(config_dir, epoch)
                        #print(sketch, pcap, flowkey, config, epoch)

                        input_path = epoch_dir

                        args_list.append((input_path, rows, width, levels, args.dataplane))

    prh = ParallelRunHelper(30)
    for process_args in args_list:
        prh.call(run_parallel_helper_function, process_args)
    prh.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataplane', required=True)
    parser.add_argument('--project_name', default='QuerySketch')
    args = parser.parse_args()
    main(args)
