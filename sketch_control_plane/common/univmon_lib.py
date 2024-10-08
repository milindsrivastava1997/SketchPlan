from sw_dp_simulator.hash_module.py.hash import compute_hash
#from sketch_control_plane.common.common import counter_estimate_cs
from sketch_control_plane.QuerySketch.select_params.sketch.common import counter_estimate_cs

#def counter_estimate(key, sketch_array, index_hash_sub_list, res_hash_sub_list, d, w, hash, level, compact_hash = False):
#    a = []
#
#    if compact_hash == False:
#        for i in range(0, d):
#            index = compute_hash(key, hash, index_hash_sub_list[i], w)
#            res = compute_hash(key, hash, res_hash_sub_list[i], 2)
#            res = res * 2 - 1
#            estimate = sketch_array[i * w + index] * res
#            a.append(estimate)
#
#    else:
#        long_hash = compute_hash(key, hash, res_hash_sub_list[0], 4294967296)
#        for i in range(0, d):
#            index = compute_hash(key, hash, index_hash_sub_list[i], w)
#            res = (long_hash>>i) & 1
#            res = res * 2 - 1
#
#            estimate = sketch_array[i * w + index] * res
#            a.append(estimate)
#
#    a.sort()
#
#    middle = int(d/2)
#
#    if d % 2 == 0:
#        return int((a[middle] + a[middle-1]) / 2)
#    else:
#        return int(a[middle])

def get_last_level(sh):
    last_level = 0
    for i in range(15,-1,-1):
        if ((sh >> i) & 1) == 0:
            return last_level
        last_level += 1
    return last_level

def get_last_level_xxhash(flowkey, level):
    params = {'row': 199}
    hash_range = 2**32 - 1
    hash_value = compute_hash(flowkey, 'xxhash_hash', params, hash_range)
    hash_value_str = bin(hash_value)[2:].zfill(32)
    leading_zeros = (hash_value_str + '1').index('1')
    return min(level - 1, leading_zeros)


def last_level_lib(compact_hash, flowkey, hash_function, sampling_hash_list, l):
    last_level = 0
    if hash_function == 'xxhash_hash':
        return get_last_level_xxhash(flowkey, l)
        
    elif hash_function == 'crc_hash':
        if compact_hash == False:
            for i in range(0, l):
                sh = compute_hash(flowkey, hash_function, sampling_hash_list[i], 33554432)
                # print(i, sh&1, sh)
                if sh & 1 == 1:
                    last_level += 1
                else:
                    break
        else:
            sh = compute_hash(flowkey, hash_function, sampling_hash_list[0], 33554432)
            last_level = get_last_level(sh)
    else:
        print('Unknown hash function:', hash_function)
        assert(False)
    
    return last_level

def hh_dict_list_sort(hh_dict_list):
    l = len(hh_dict_list)

    new_dict_list = []

    for i in range(0, l):
        wi = hh_dict_list[i]

        sorted_list = sorted(wi.items(), key=lambda kv: kv[1][0])
        sorted_list.reverse()

        new_dict = {}
        for e in sorted_list:
            new_dict[e[0]] = e[1]
        new_dict_list.append(new_dict)

    return new_dict_list

def trim_by_topk(hh_dict_list, t):
    l = len(hh_dict_list)

    topk_hh_dict_list = []
    for i in range(0, l):
        wi = hh_dict_list[i]

        topk_hh_dict = {}

        for z, (k, v) in enumerate(wi.items()):
            if z < t:
                topk_hh_dict[k] = v

        topk_hh_dict_list.append(topk_hh_dict)

    return topk_hh_dict_list

def create_estimate_hh_dict_list(return_dict, array, d, w, l, hash, compact_hash):

    sampling_hash_list = return_dict["sampling_hash_list"]
    index_hash_list = return_dict["index_hash_list"]
    res_hash_list = return_dict["res_hash_list"]
    sketch_array_list = return_dict["sketch_array_list"]
    gt_list = return_dict["gt"]
    # topk_200_list = return_dict["topk_200_list"]

    hh_dict_list = []
    for i in range(0, l):
        hh_dict_list.append({})

    # count = 0
    for (string_key, estimate, flowkey) in gt_list:
        # print(string_key, estimate, flowkey)
        # count += 1
        # if count % 100000 == 0:
        #     print(count)
        last_level = last_level_lib(compact_hash, flowkey, hash, sampling_hash_list, l)
        # print("last level: ",  last_level)
        if last_level >= l:
            continue

        i = last_level
        hh_dict = hh_dict_list[i]
        #estimate_from_counter = counter_estimate(flowkey, array[i], index_hash_list[i], res_hash_list[i], d, w, hash, i, compact_hash)        
        estimate_from_counter = counter_estimate_cs(flowkey, array[i], index_hash_list[i], res_hash_list[i], d, w, hash, compact_hash)
        hh_dict[string_key] = (estimate_from_counter, 0)
        for i in range(0, last_level):
            hh_dict = hh_dict_list[i]
            hh_dict[string_key] = (estimate_from_counter, 1)

    sorted_hh_dict_list = hh_dict_list_sort(hh_dict_list)

    return sorted_hh_dict_list

def create_estimate_hh_dict_list_using_pq(return_dict, array, d, w, l, hash, compact_hash):
    sampling_hash_list = return_dict["sampling_hash_list"]
    index_hash_list = return_dict["index_hash_list"]
    res_hash_list = return_dict["res_hash_list"]
    sketch_array_list = return_dict["sketch_array_list"]
    # gt_list = return_dict["gt"]
    topk_200_list = return_dict["topk_200_list"]

    hh_dict_list = []
    for i in range(0, l):
        # print("level", i)
        hh_dict_list.append({})
        hh_dict = hh_dict_list[i]
        for (string_key, estimate, flowkey) in topk_200_list[i]:
            # print(string_key, estimate, flowkey)
            # last_level = last_level_lib(compact_hash, flowkey, hash, sampling_hash_list, l)
            hh_dict[string_key] = (estimate, 0)
            for j in range(0, i):
                hh_dict_list[j][string_key] = (estimate, 1)

            # print("last_level", last_level)
            # print("current level", i)
            # if last_level == i:
            #     print(estimate, 0, last_level)
            # else:
            #     hh_dict[string_key] = (estimate, 1)
            #     print(estimate, 1, last_level)
    sorted_hh_dict_list = hh_dict_list_sort(hh_dict_list)

    return sorted_hh_dict_list
    

