from sw_dp_simulator.file_io.py.read_cm import load_cm
from sw_dp_simulator.hash_module.py.hash import compute_hash
#from sketch_control_plane.common.common import counter_estimate_cm
from sketch_control_plane.common.metric_classes import Metric
from sketch_control_plane.QuerySketch.select_params import constants
from sketch_control_plane.QuerySketch.select_params.sketch.common import counter_estimate_cm, get_ARE_from_counters, get_entropy

import math
from collections import defaultdict

def relative_error(true, estimate):
    if true == 0:
        return 0
    return abs(true-estimate)/true*100

#def counter_estimate(key, sketch_array, index_hash_sub_list, res_hash_sub_list, d, w, hash, level):
#    a = []
#
#    for i in range(0, d):
#        #index = compute_hash(key, hash, index_hash_sub_list[i], w)
#        index = compute_hash(key, hash, {'row': i}, w)
#        estimate = sketch_array[i * w + index]
#        a.append(estimate)
#

#def get_ARE(result, cArray, d, w, hash_function):
#
#    gt_result = result["gt"]
#    index_hash_list = result["index_hash_list"]
#    res_hash_list = result["res_hash_list"]
#    sketch_array_list = result["sketch_array_list"]
#
#    topk = 100
#    s = 0
#    for i in range(0, min(topk, len(gt_result))):
#        true_flow_count = gt_result[i][1]
#        flowkey = gt_result[i][2]
#        est = counter_estimate_cm(flowkey, cArray, index_hash_list[0], res_hash_list[0], d, w, hash_function)
#        error = relative_error(true_flow_count, est)
#        s += error
#
#    # print("ARE: ", sum / topk)
#    ret = Metric('hh', None, None, s/topk)
#    return ret
#    return s / topk

def compare_pq(item1, item2):
    if item1[0] < item2[0]:
        return 1
    elif item1[0] > item2[0]:
        return -1
    else:
        return 0

def get_change_detection(output_dir, result, cArray, row, arow, width, hash_function, topk, dataplane):
    # get current epoch ground truth result
    gt_result = result["gt"]
    index_hash_list = result["index_hash_list"]
    res_hash_list = result["res_hash_list"]

    epoch = output_dir.split('seed_')[1]
    epoch = int(epoch.split('/')[1])
    # epoch 1 - nothing (epoch 0)
    if epoch == 1:
        #ret = get_ARE(result, cArray, arow, width, hash_function)
        ret = get_ARE_from_counters(result, arow, width, hash_function, topk, counter_estimate_cm)
        ret.name = 'cd'
        return ret
    else:
        output_dir_prev = output_dir[:-2]
        output_dir_prev += str(epoch - 1).zfill(2)
        result_prev = load_cm(output_dir_prev, width, row, dataplane)

        # print(result_prev['gt'][0][1])
        # print(result_prev['gt'][0][2])
        gt_result_prev = result_prev['gt']
        index_hash_list_prev = result_prev["index_hash_list"]
        res_hash_list_prev = result_prev["res_hash_list"]
        cArray_prev = result_prev["sketch_array_list"][0]

        ll = []
        # Find the change in the ground truth (current epoch - previous epoch)
        for i in range(0, len(gt_result)):
            true_flow_count = gt_result[i][1]
            flowkey = gt_result[i][2]
            isMatch = False

            # find the same flow key in the previous epoch
            for j in range(0, len(gt_result_prev)):
                flowkey_prev = gt_result_prev[j][2]
                if flowkey == flowkey_prev:
                    isMatch = True
                    break
            if isMatch:
                change = abs(gt_result_prev[j][1] - true_flow_count)
            else:
                change = abs(true_flow_count)
            ll.append((change, flowkey))
        
        # sort list from big to small by flow change
        import functools
        pq = sorted(ll, key=functools.cmp_to_key(compare_pq))

        #topk = 100
        s = 0
        true_changes = []
        est_changes = []
        length = min(topk, len(gt_result))

        # Calculate the topk ARE for change detection
        for i in range(length):
            flowkey = pq[i][1]
            true_change = pq[i][0]
            #est = counter_estimate(flowkey, cArray, index_hash_list[0], res_hash_list[0], arow, width, hash_function, 0)
            est = counter_estimate_cm(flowkey, cArray, index_hash_list[0], res_hash_list[0], arow, width, hash_function)
            #est_prev = counter_estimate(flowkey, cArray_prev, index_hash_list_prev[0], res_hash_list_prev[0], arow, width, hash_function, 0)
            est_prev = counter_estimate_cm(flowkey, cArray_prev, index_hash_list_prev[0], res_hash_list_prev[0], arow, width, hash_function)
            est_change = abs(est - est_prev)
            true_changes.append(true_change)
            est_changes.append(est_change)
            error = relative_error(true_change, est_change)
            s += error

        #ret = Metric('cd', None, None, s/topk)
        ret = Metric('cd', true_changes, est_changes, s/topk)
        return ret
        #return s / topk

def bin_data(dist, bin_size):
    res = defaultdict(int)
    for key, value in dist.items():
        bin_idx = key // bin_size
        res[bin_idx] += value
    return res

def get_flow_size_distribution(result, row, width, hash_function, new=False, bin_size=None):
    cArray = result["sketch_array_list"][0]
    gt_result = result["gt"]
    index_hash_list = result["index_hash_list"]
    res_hash_list = result["res_hash_list"]

    true_distribution = {}
    est_distribution = {}
    for i in range(0, len(gt_result)):
        true_flow_count = gt_result[i][1]
        flowkey = gt_result[i][2]

        if true_flow_count in true_distribution:
            true_distribution[true_flow_count] += 1
        else:
            true_distribution[true_flow_count] = 1

        #est = counter_estimate(flowkey, cArray, index_hash_list[0], res_hash_list[0], row, width, hash_function, 0)
        est = counter_estimate_cm(flowkey, cArray, index_hash_list[0], res_hash_list[0], row, width, hash_function)
        if est in est_distribution:
            est_distribution[est] += 1
        else:
            est_distribution[est] = 1
    
    # print(est_distribution)
    if new:
        if bin_size is None:
            bin_size = 1
        binned_true_distribution = bin_data(true_distribution, bin_size)
        binned_est_distribution = bin_data(est_distribution, bin_size)
    else:
        binned_true_distribution = true_distribution
        binned_est_distribution = est_distribution    

    WMRD_nom = 0
    WMRD_denom = 0

    for key in binned_true_distribution:
        true = binned_true_distribution[key]
        if key in binned_est_distribution:
            est = binned_est_distribution[key]
        else:
            est = 0

        WMRD_nom += abs(true - est)
        WMRD_denom += float(true + est)/2
    WMRD = WMRD_nom/WMRD_denom    
    WMRD *= 100
    
    # print(f'WMRD: {WMRD}')
    #ret = Metric('fsd', None, None, WMRD)
    if new:
        ret = Metric('fsd_new', binned_true_distribution, binned_est_distribution, WMRD)
    else:
        ret = Metric('fsd', binned_true_distribution, binned_est_distribution, WMRD)
    return ret
    #return WMRD


def cm_main(sketch_name, output_dir, row, width, level, arow, dataplane, topk, bin_size):
    result = load_cm(output_dir, width, row, dataplane)
    hash_function = constants.DP_HASH_MAP[dataplane]
    sim_total = result["sketch_array_list"][0]

    ret = {}

    #entropy_error = get_entropy(result, sim_total, arow, width)
    ret['ent'] = get_entropy(result, sim_total, arow, width)
    #ret = entropy_error

    #sim_ARE_error = get_ARE(result, sim_total, arow, width, hash_function)
    #sim_ARE_error = get_ARE_from_counters(result, arow, width, hash_function, topk, counter_estimate_cm)
    ret['hh'] = get_ARE_from_counters(result, arow, width, hash_function, topk, counter_estimate_cm)
    #ret.append(sim_ARE_error)

    #sim_change_detection_error = get_change_detection(output_dir, result, sim_total, row, arow, width, hash_function, topk)
    ret['cd'] = get_change_detection(output_dir, result, sim_total, row, arow, width, hash_function, topk, dataplane)
    #ret.append(sim_change_detection_error)

    #WMRD = get_flow_size_distribution(result, arow, width, hash_function)
    ret['fsd'] = get_flow_size_distribution(result, arow, width, hash_function)
    ret['fsd_new'] = get_flow_size_distribution(result, arow, width, hash_function, new=True, bin_size=bin_size)
    #ret.append(WMRD)

    return ret
