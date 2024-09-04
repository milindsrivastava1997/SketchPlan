import math
import statistics
from sw_dp_simulator.hash_module.py.hash import compute_hash
from sketch_control_plane.common.gsum_lib import ground_truth_entropy
from sketch_control_plane.common.metric_classes import Metric

def get_miss_rate(gt, flowkey, topk):
    true = 0
    false = 0

    for i in range(0, topk):
        gt_elem = gt[i]
        find = False
        for hh_elem in flowkey:
            if str(gt_elem[2]) == str(hh_elem[2]):
                find = True
                break
        if find == True:
            true+=1
        else:
            false+=1
    miss_rate = (false / topk)*100
    return miss_rate

def relative_error(true, estimate):
    if true == 0:
        return 0
    return abs(true-estimate)/true*100

def get_ARE(gt, compare, topk):
    s = 0
    for i in range(min(topk, len(gt))):
        gt_elem = gt[i]
        error = relative_error(gt_elem[1], compare[i])
        s += error
    ARE = s/topk
    return ARE

def get_ARE_from_counters(result, rows, width, hash_function, topk, counter_estimation_function):
    gt_result = result["gt"]
    sampling_hash_list = result["sampling_hash_list"]
    index_hash_list = result["index_hash_list"]
    res_hash_list = result["res_hash_list"]
    sketch_array_list = result["sketch_array_list"]

    true_counts = []
    estimated_counts = []

    s = 0
    for i in range(0, min(topk, len(gt_result))):
        true_flow_count = gt_result[i][1]
        flowkey = gt_result[i][2]
        est = counter_estimation_function(flowkey, sketch_array_list[0], index_hash_list[0], res_hash_list[0], rows, width, hash_function, True)
        true_counts.append(true_flow_count)
        estimated_counts.append(est)
        error = relative_error(true_flow_count, est)
        s += error

    ret = Metric('hh', true_counts, estimated_counts, s/topk)
    return ret
    #return s/topk

def counter_estimate_cs(key, sketch_array, index_hash_sub_list, res_hash_sub_list, rows, width, hash_function, compact_hash = False):
    a = []

    if hash_function == 'anup_hash':
        for row in range(rows):
            index = compute_hash(key, hash_function, {'row': row}, width)
            res = compute_hash(key, hash_function, {'row': row}, 2)
            res = res * 2 - 1
            estimate = sketch_array[row * width + index] * res
            a.append(estimate)

    elif hash_function == 'xxhash_hash':
        for row in range(rows):
            # NOTE: based on old hashing scheme for DPDK/XDP implementations
            #computed_hash = compute_hash(key, hash_function, {'row': row}, width)
            #index = (computed_hash >> 1) % width
            #res = computed_hash & 1
            #res = 1 - res * 2
            # NOTE: based on new hashing scheme for DPDK/XDP implementations
            index_hash = compute_hash(key, hash_function, {'row': row}, width)
            res_hash = compute_hash(key, hash_function, {'row': row + rows}, 2)
            index = index_hash % width
            res = 1 - res_hash * 2 
            estimate = sketch_array[row * width + index] * res
            a.append(estimate)

    elif hash_function == 'crc_hash':
        if compact_hash == False:
            for row in range(rows):
                index = compute_hash(key, hash_function, index_hash_sub_list[row], width)
                res = compute_hash(key, hash_function, res_hash_sub_list[row], 2)
                res = res * 2 - 1
                estimate = sketch_array[row * width + index] * res
                a.append(estimate)

        else:
            long_hash = compute_hash(key, hash_function, res_hash_sub_list[0], 4294967296)
            for row in range(rows):
                index = compute_hash(key, hash_function, index_hash_sub_list[row], width)
                res = (long_hash >> row) & 1
                res = res * 2 - 1

                estimate = sketch_array[row * width + index] * res
                a.append(estimate)
    else:
        print('Unknown hash function:', hash_function)
        assert(False)

    return statistics.median(a)

    #a.sort()

    #middle = int(d/2)

    #if d % 2 == 0:
    #    return int((a[middle] + a[middle-1]) / 2)
    #else:
    #    return int(a[middle])

def counter_estimate_cm(key, sketch_array, index_hash_sub_list, res_hash_sub_list, rows, width, hash_function, compact_hash=False):
    a = []

    if hash_function == 'anup_hash':
        for row in range(rows):
            index = compute_hash(key, hash_function, {'row': row}, width)
            estimate = sketch_array[row * width + index]
            a.append(estimate)
    elif hash_function == 'xxhash_hash':
        for row in range(rows):
            index = compute_hash(key, hash_function, {'row': row}, width)
            estimate = sketch_array[row * width + index]
            a.append(estimate)
    elif hash_function == 'crc_hash':
        for row in range(rows):
            index = compute_hash(key, hash_function, index_hash_sub_list[row], width)
            estimate = sketch_array[row * width + index]
            a.append(estimate)
    else:
        print('Unknown hash function:', hash_function)
        assert(False)

    return min(a)

def get_entropy(result, cArray, rows, width):
    total = result["count_list"][0]
    #entropy = ground_truth_entropy(result['gt'])[1]
    entropy = result['entropy']

    a = []
    for i in range(rows):
        # total2 = 0
        # for j in range(0, w):
        #     cij = abs(cArray[i*w + j])
        #     total2 += cij
        # print(total, total2)

        entropy_est = 0
        for j in range(width):
            cij = abs(cArray[i*width + j])
            p = (cij/total)
            if p != 0:
                entropy_est += p * math.log2(p)
        entropy_est *= -1
        a.append(entropy_est)
        # break
    entropy_est = statistics.median(a)

    # print("use counters:", [entropy, entropy_est, relative_error(entropy, entropy_est)])
    # a = []
    # for i in range(0, d):
    #     sum = 0
    #     test_sum = 0
    #     for j in range(0, w):
    #         # print(cArray[i*w + j])
    #         cij = abs(cArray[i*w + j])
    #         test_sum += cij
    #         if cij >= 3:
    #             X = total * (cij * math.log2(cij) - (cij-1) * math.log2((cij-1)))
    #             # X = total * (cij * math.log2(cij) - cij_minus * math.log2(cij_minus))
    #             sum += X
    #     sum = sum / w
    #     a.append(sum)
    # entropy_est = statistics.median(a)
    # entropy_est = math.log2(total) - entropy_est/total

    # print("use sketch:", [entropy, entropy_est, relative_error(entropy, entropy_est)])

    ret = Metric('ent', entropy, entropy_est, relative_error(entropy, entropy_est))
    return ret
