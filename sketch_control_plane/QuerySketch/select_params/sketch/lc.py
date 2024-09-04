from sw_dp_simulator.file_io.py.read_lc import load_lc
from sketch_control_plane.common.metric_classes import Metric

def relative_error(true, estimate):
    if true == 0:
        return 0
    return abs(true-estimate)/true*100

def bitzero(M):
    count = 0
    for item in M:
        if item == 0:
            count+=1
    return count

def get_cardinality(M, width):
    z = bitzero(M)
    # prevent division by zero
    if z == 0:
        z = 1
    import math
    return width * math.log(width/z)

def lc_main(sketch_name, output_dir, row, width, level, arow, dataplane, topk):
    ret = {}
    result = load_lc(output_dir, width, 1, dataplane)
    true_cardinality = result["cardinality"]
    sim_cardinality = get_cardinality(result["sketch_array_list"][0], width)
    sim_error = relative_error(true_cardinality, sim_cardinality)
    # print(true_cardinality, int(sim_cardinality), sim_error)
    print("true(%d) est(%d) error(%.2f%%)" % (true_cardinality, sim_cardinality, sim_error))
    ret['cardinality'] = Metric('cardinality', true_cardinality, sim_cardinality, sim_error)
    return ret
