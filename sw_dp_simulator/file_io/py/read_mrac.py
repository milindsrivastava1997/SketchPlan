from sw_dp_simulator.file_io.py.common import parse_line
import os

def load_mrac(dir, w, d, l, dataplane):

    hh_dict_list = []
    count_list = []

    sampling_hash_list = []
    res_hash_list = []
    index_hash_list = []

    sketch_array_list = []

    for level in range(0, l):
        file_path = '%s/level_%02d/' % (dir, level)

        f = open(file_path + "total.txt")
        line = f.readline().strip()
        count_list.append(int(line))
        f.close()

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
        # print(key)
        for line in f:
            string_key, estimate, flowkey = parse_line(key, line.strip())
            gt_result.append((string_key, estimate, flowkey))

    f = open('%s/entropy.txt' % dir)
    line = f.readline().strip()
    entropy = float(line)
    f.close()

    # flowkey_result = []
    # with open('%s/flowkey.txt' % dir) as f:
    #     key = f.readline().strip()
    #     # print(key)
    #     for line in f:
    #         string_key, estimate, flowkey = parse_line(key, line.strip())
    #         flowkey_result.append((string_key, estimate, flowkey))

    return_dict = {}
    return_dict["gt"] = gt_result
    return_dict["entropy"] = entropy
    # return_dict["flowkey"] = flowkey_result

    return_dict["count_list"] = count_list

    return_dict["sampling_hash_list"] = sampling_hash_list
    return_dict["index_hash_list"] = index_hash_list
    return_dict["res_hash_list"] = res_hash_list

    return_dict["sketch_array_list"] = sketch_array_list

    return return_dict
