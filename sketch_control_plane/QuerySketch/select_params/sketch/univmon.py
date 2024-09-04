from sketch_control_plane.common.gsum_lib import ground_truth_entropy, ground_truth_cardinality
from sketch_control_plane.common.common import relative_error
from sketch_control_plane.common.univmon_lib import create_estimate_hh_dict_list, trim_by_topk, create_estimate_hh_dict_list_using_pq, counter_estimate_cs, last_level_lib
from sketch_control_plane.common.gsum_lib import estimate_entropy_gsum, estimate_cardinality_gsum
from sketch_control_plane.common.metric_classes import Metric
from sw_dp_simulator.file_io.py.read_univmon import load_univmon
from sketch_control_plane.QuerySketch.select_params.sketch.common import get_miss_rate, get_ARE
from sketch_control_plane.QuerySketch.select_params import constants

def univmon_main(sketch_name, output_dir, row, width, level, arow, dataplane, topk):
    #local_topk = 50
    ret = {}
    #local_topk = topk
    local_topk = 50
    result = load_univmon(output_dir, None, width, row, level, dataplane, load_top200=True)
    hash_function = constants.DP_HASH_MAP[dataplane]

    gt_result_list = result["gt"]

    #hh_list = create_estimate_hh_dict_list(result, result["sketch_array_list"], row, width, level, "crc_hash", False)
    #hh_list = create_estimate_hh_dict_list_using_pq(result, result["sketch_array_list"], row, width, level, "crc_hash", False)
    #hh_list = create_estimate_hh_dict_list(result, result["sketch_array_list"], row, width, level, hash_function, True)
    hh_list = create_estimate_hh_dict_list_using_pq(result, result["sketch_array_list"], row, width, level, hash_function, True)
    hh_list = trim_by_topk(hh_list, topk)
    # for k,v in hh_list[0].items():
    #     print(k, v)

    total, true_entropy = ground_truth_entropy(gt_result_list)

    # total = 734381062
    # true_entropy = 17.543519

    sim_entropy = estimate_entropy_gsum(hh_list, total)
    sim_entropy_error = relative_error(true_entropy, sim_entropy)

    ret['ent'] = Metric('ent', true_entropy, sim_entropy, sim_entropy_error)

    total, true_card = ground_truth_cardinality(gt_result_list)

    # true_card = 30000000
    sim_card = estimate_cardinality_gsum(hh_list)
    sim_card_error = relative_error(true_card, sim_card)
    ret['cardinality'] = Metric('cardinality', true_card, sim_card, sim_card_error)
    
    # print(hh_list[0])
    #a = []
    #for idx, (k, v) in enumerate(hh_list[0].items()):
    #    # print(k, v)
    #    a.append(v[0])
    #    #print("ARE: {} {}".format(gt_result_list[idx][1], v[0]))
    ## print(a)
    #ARE = get_ARE(gt_result_list, a, local_topk)
    #print("[UnivMon] ARE: %f%%" % ARE)
    #ret['hh'] = Metric('hh', None, None, ARE)

    ## milind start
    s = 0
    true_counts = []
    estimated_counts = []
    for i in range(0, min(local_topk, len(gt_result_list))):
        true_count = gt_result_list[i][1]
        flowkey = gt_result_list[i][2]
        max_est = counter_estimate_cs(flowkey, result["sketch_array_list"][0], result["index_hash_list"][0], result["res_hash_list"][0], row, width, hash_function, True)
        true_counts.append(true_count)
        estimated_counts.append(max_est)
        error = relative_error(true_count, max_est)
        s += error
    #print("[UnivMon] ARE first layer: %f%%" % (s/local_topk))
    #ret['hh_milind_first'] = Metric('hh', None, None, s/local_topk)
    ret['hh_milind_first'] = Metric('hh', true_counts, estimated_counts, s/local_topk)
    s = 0
    true_counts = []
    estimated_counts = []
    for i in range(0, min(local_topk, len(gt_result_list))):
        true_count = gt_result_list[i][1]
        flowkey = gt_result_list[i][2]
        max_est = float('-inf')
        max_level = last_level_lib(True, flowkey, hash_function, result['sampling_hash_list'], level)
        #print('Index: {}, last level {}'.format(i, max_level))
        for l in range(level):
            est = counter_estimate_cs(flowkey, result["sketch_array_list"][l], result["index_hash_list"][l], result["res_hash_list"][l], row, width, hash_function, True)
            #print("Mine: {} {}".format(true_count, est))
            if est > max_est:
                max_est = est
            #print('Level: {}, est: {}'.format(l, est))
        true_counts.append(true_count)
        estimated_counts.append(max_est)
        error = relative_error(true_count, max_est)
        s += error
        #print('-' * 20)
    #print("[UnivMon] ARE max: %f%%" % (s/local_topk))
    #ret['hh_milind_max'] = Metric('hh', None, None, s/local_topk)
    ret['hh_milind_max'] = Metric('hh', true_counts, estimated_counts, s/local_topk)
    s = 0
    true_counts = []
    estimated_counts = []
    for i in range(0, min(local_topk, len(gt_result_list))):
        true_count = gt_result_list[i][1]
        flowkey = gt_result_list[i][2]
        max_level = last_level_lib(True, flowkey, hash_function, result['sampling_hash_list'], level)
        max_est = counter_estimate_cs(flowkey, result["sketch_array_list"][max_level], result["index_hash_list"][max_level], result["res_hash_list"][max_level], row, width, hash_function, True)
        true_counts.append(true_count)
        estimated_counts.append(max_est)
        error = relative_error(true_count, max_est)
        s += error
    #print("[UnivMon] ARE last layer: %f%%" % (s/local_topk))
    #ret['hh_milind_last'] = Metric('hh', None, None, s/local_topk)
    ret['hh_milind_last'] = Metric('hh', true_counts, estimated_counts, s/local_topk)
    ## milind end

    #return [true_entropy, sim_entropy, sim_entropy_error, true_card, sim_card, sim_card_error, ARE]
    return ret
