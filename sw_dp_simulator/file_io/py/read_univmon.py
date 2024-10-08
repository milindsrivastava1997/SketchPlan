from sw_dp_simulator.file_io.py.common import parse_line
import os

def load_univmon(dir, t, w, d, l, dataplane, load_top200):

    hh_dict_list = []
    count_list = []

    sampling_hash_list = []
    res_hash_list = []
    index_hash_list = []

    sketch_array_list = []
    topk_200_list = []
    flowkey_result = []

    for level in range(0, l):
        file_path = '%s/level_%02d/' % (dir, level)

        f = open(file_path + "total.txt")
        line = f.readline().strip()
        count_list.append(int(line))
        f.close()

        if dataplane == 'sw':
            f = open(file_path + "sampling_hash_params.txt")
            f.readline().strip()
            pline = f.readline().strip()
            aParam = int(pline.split(" ")[0])
            bParam = int(pline.split(" ")[1])
            polyParam = int(pline.split(" ")[2])
            initParam = int(pline.split(" ")[3])
            xoutParam = int(pline.split(" ")[4])
            sampling_hash_list.append((aParam, bParam, polyParam, initParam, xoutParam))
            f.close()

            sub_list = []
            f = open(file_path + "index_hash_params.txt")
            for i in range(0, d):
                pline = f.readline().strip()
                aParam = int(pline.split(" ")[0])
                bParam = int(pline.split(" ")[1])
                polyParam = int(pline.split(" ")[2])
                initParam = int(pline.split(" ")[3])
                xoutParam = int(pline.split(" ")[4])
                sub_list.append((aParam, bParam, polyParam, initParam, xoutParam))
            f.close()
            index_hash_list.append(sub_list)

            sub_list = []
            f = open(file_path + "res_hash_params.txt")
            for i in range(0, d):
                pline = f.readline().strip()
                aParam = int(pline.split(" ")[0])
                bParam = int(pline.split(" ")[1])
                polyParam = int(pline.split(" ")[2])
                initParam = int(pline.split(" ")[3])
                xoutParam = int(pline.split(" ")[4])
                sub_list.append((aParam, bParam, polyParam, initParam, xoutParam))
            f.close()
            res_hash_list.append(sub_list)

        else:
            sampling_hash_list.append(None)
            index_hash_list.append(None)
            res_hash_list.append(None)

        if load_top200:
            sub_list = []
            f = open(file_path + "top_200.txt")
            key = f.readline().strip()
            for line in f:
                string_key, estimate, flowkey = parse_line(key, line.strip())
                sub_list.append((string_key, estimate, flowkey))
            f.close()
            topk_200_list.append(sub_list)
        else:
            topk_200_list.append(None)
        
        sub_list = []
        f = open(file_path + "sketch_counter.txt")
        for i in range(0, d * w):
            pline = f.readline().strip()
            sub_list.append(int(pline))
        f.close()
        sketch_array_list.append(sub_list)

    gt_result = []
    with open('%s/ground_truth.txt' % dir) as f:
        key = f.readline().strip()
        for line in f:
            string_key, estimate, flowkey = parse_line(key, line.strip())
            gt_result.append((string_key, estimate, flowkey))

    if dataplane == 'sw':
        with open('%s/flowkey.txt' % dir) as f:
            key = f.readline().strip()
            for line in f:
                string_key, estimate, flowkey = parse_line(key, line.strip())
                flowkey_result.append((string_key, estimate, flowkey))

    return_dict = {}
    return_dict["gt"] = gt_result
    return_dict["count_list"] = count_list
    return_dict["sketch_array_list"] = sketch_array_list

    #if dataplane == 'sw':
    return_dict["flowkey"] = flowkey_result
    return_dict["sampling_hash_list"] = sampling_hash_list
    return_dict["index_hash_list"] = index_hash_list
    return_dict["res_hash_list"] = res_hash_list
    return_dict["topk_200_list"] = topk_200_list

    return return_dict
