import gurobipy as gp
from gurobipy import GRB

import sys
import pickle
import itertools
import os
import random
import json
import time
import numpy as np
import argparse
import multiprocessing
import concurrent.futures
from collections import defaultdict 

import constants

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main_constants import *
import utils

path = os.path.join(ROOT_DIR, 'sketch_metric_coverage','actual.pkl')
profiles_path = PROFILES_PATH
#metric_max_json_file = os.path.join(PROFILES_PATH, 'metric_max.json')

# either define OD pairs or randomly generate them
random.seed(0)
#DEVICE_SRAM = 2**19
DEVICE_SRAM = None
OD_PAIRS = [
#(0, 1, 2),
#(2, 3, 4),
#(0, 1, 5, 6)
]
OD_PAIRS = [tuple(random.sample(range(0, 200), random.randint(2, 10))) for _ in range(0, 20)]
#OD_PAIRS = [tuple(random.sample(range(0, 20), random.randint(2,6))) for _ in range(0, 5)]
#OD_PAIRS = [(0, 1)]
#print(OD_PAIRS)

NUM_OD_PAIRS = len(OD_PAIRS)
ALL_DEVICES = []
for od_pair in OD_PAIRS:
    ALL_DEVICES += od_pair
ALL_DEVICES = list(set(ALL_DEVICES))
NUM_DEVICES = max(ALL_DEVICES) + 1

DEVICES = ['device{}'.format(i) for i in range(0, NUM_DEVICES)]
DEVICE_RAMS = [DEVICE_SRAM for i in range(0, NUM_DEVICES)]

# randomly generate clusters
NUM_CLUSTERS = 5
CLUSTER_DEVICE_MAP = [[] for _ in range(0, NUM_CLUSTERS)]
for idx in range(NUM_DEVICES):
    cluster_idx = random.randint(0, NUM_CLUSTERS - 1)
    CLUSTER_DEVICE_MAP[cluster_idx].append(idx)

# trivial clustering, each device is a cluster
#CLUSTER_DEVICE_MAP = [[i] for i in range(NUM_DEVICES)]

# trviial clustering, all devices in one cluster
#CLUSTER_DEVICE_MAP = [list(range(NUM_DEVICES))]
NUM_CLUSTERS = len(CLUSTER_DEVICE_MAP)

METRICS = None
#METRICS = ['hh', 'ent', 'cardinality', 'cd', 'fsd']
#METRICS = ['ent', 'fsd', 'hh']

# DEFAULT
#ERROR_BOUNDS = {
#    'hh': 5,
#    'ent': 5,
#    'cardinality': 3,
#    'cd': 5,
#    'fsd': 50,
#}

# FOR STRAWMAN, LOOSE
#ERROR_BOUNDS = {
#    'hh': 7,
#    'ent': 20,
#    'cardinality': 10,
#    'cd': 10,
#    'fsd': 75,
#}

# FOR STRAWMAN, MEDIUM 
#ERROR_BOUNDS = {
#    'hh': 5,
#    'ent': 15,
#    'cardinality': 7,
#    'cd': 7,
#    'fsd': 70,
#}

SKETCHES = ['cm', 'cs', 'hll', 'lc', 'll', 'mrac', 'mrb', 'univmon']
#SKETCHES = ['cs', 'univmon']

def get_metric_to_sketch_maps():
    metric_to_sketch_data = None
    with open(path, 'rb') as fin:
        metric_to_sketch_data = pickle.load(fin)

    metric_to_available_sketches_map = {}

    for metric in METRICS:
        metric_to_available_sketches_map[metric] = sorted(list(set(SKETCHES) & set(metric_to_sketch_data['metric_to_sketch'][metric])))

    iterables = [[(m, v) for v in metric_to_available_sketches_map[m]] for m in metric_to_available_sketches_map.keys()]
    product = itertools.product(*iterables)

    result = []
    for p in product:
        result.append({m:s for (m, s) in p})
    return result, metric_to_available_sketches_map

# invert maps 
idx_sketch_map = {v: k for k, v in constants.sketch_idx_map.items()}
idx_metric_map = {v: k for k, v in constants.metric_idx_map.items()}

def get_metric_to_sketch_maps_strawman(strawman_json_file, strawman_name, seed):
    json_data = json.load(open(strawman_json_file, 'r'))
    strawman_name_tokens = strawman_name.split(':')

    strawman_solutions = [d['solutions'] for d in json_data if strawman_name_tokens[0] in d['name'] and strawman_name_tokens[1] in d['name'] and d['name'].endswith(':{}'.format(seed))]
    strawman_solution = strawman_solutions[0]
    
    metric_to_sketch_map = {}
    metric_to_available_sketches_map = {}
    
    for sketch_deployment in strawman_solution:
        if isinstance(sketch_deployment['algo'], int):
            # old format
            sketch = idx_sketch_map[sketch_deployment['algo']]
            metrics = [idx_metric_map[m] for m in sketch_deployment['metric']]
        else:
            sketch = sketch_deployment['algo']
            metrics = [query['metric'] for query in sketch_deployment['query']]

        for metric in metrics:
            metric_to_sketch_map[metric] = sketch
            metric_to_available_sketches_map[metric] = [sketch]
    return [metric_to_sketch_map], metric_to_available_sketches_map

def flatten_profiles(profiles):
    # input: profiles[sketch][metric][levels][row][width]
    # output: profiles[sketch][metric][(levels, row, width)]
    result = {}
    for sketch in profiles.keys():
        result[sketch] = {}
        for metric in profiles[sketch].keys():
            result[sketch][metric] = {}
            for levels in profiles[sketch][metric].keys():
                for row in profiles[sketch][metric][levels].keys():
                    for width in profiles[sketch][metric][levels][row].keys():
                        result[sketch][metric][(levels, row, width)] = profiles[sketch][metric][levels][row][width]
    return result

def optimal_baseline_placement(metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, od_pairs, devices, device_rams, od_pair_metric_map, run_strawman=False, time_limit=0):
    m = gp.Model()
    m.params.OutputFlag = 0
    if time_limit > 0:
        m.params.TimeLimit = time_limit

    num_unique_sketches = len(unique_sketches)
    num_profiles_per_sketch = []

    print(metric_min_sketch_resource_config)

    if run_strawman:
        new_profiles = {}
        for sketch in unique_sketches:
            for metric in list(profiles[sketch].keys()):
                print(sketch, metric, metric in metric_min_sketch_resource_config, metric in metric_sketch_map, metric_sketch_map[metric] == sketch if metric in metric_sketch_map else False)
                if metric in metric_min_sketch_resource_config and metric in metric_sketch_map and metric_sketch_map[metric] == sketch:
                    if sketch not in new_profiles:
                        new_profiles[sketch] = {}
                    new_profiles[sketch][metric] = {metric_min_sketch_resource_config[metric]: 0}

        for sketch in unique_sketches:
            for metric in list(profiles[sketch].keys()):
                if metric not in new_profiles[sketch]:
                    # find some other metric from new_profiles[sketch]
                    other_metric = list(new_profiles[sketch].keys())[0]
                    new_profiles[sketch][metric] = new_profiles[sketch][other_metric]

        profiles = new_profiles

    for sketch in unique_sketches:
        this_sketch_metrics = list(profiles[sketch].keys())
        num_resource_configs = len(profiles[sketch][this_sketch_metrics[0]])
        num_profiles_per_sketch.append(num_resource_configs)

    print('Profiles')
    print(profiles)
    print('Metric sketch map')
    print(metric_sketch_map)
    print('Unique sketches')
    print(unique_sketches)
    print('Num profiles per sketch')
    print(num_profiles_per_sketch)

    metric_unique_sketch_idx_map = {metric: unique_sketches.index(metric_sketch_map[metric]) for metric in metrics}
       
    # data plane indicators: x[device][sketch][resource]
    data_plane_indicators = [[[m.addVar(lb=0, ub=1, vtype=GRB.BINARY) for _ in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)] for _ in range(len(devices))]
    # control plane indicators: y[metric][device]
    control_plane_indicators = [[m.addVar(lb=0, ub=1, vtype=GRB.BINARY) for _ in range(len(devices))] for _ in range(len(metrics))]
    # memory variables: x[sketch][resource]
    memory = [[m.addVar(vtype=GRB.INTEGER) for _ in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)]

    # assign memory values to memory variables
    for sketch_idx in range(num_unique_sketches):
        for resource_idx in range(num_profiles_per_sketch[sketch_idx]):
            some_sketch_metric = list(profiles[unique_sketches[sketch_idx]].keys())[0]
            profile_config = list(profiles[unique_sketches[sketch_idx]][some_sketch_metric].keys())[resource_idx]
            profile_memory = np.prod(profile_config)
            m.addConstr(memory[sketch_idx][resource_idx] == profile_memory)

    # constraint: for each device, if a sketch is deployed, it should only have 1 resource configuration 
    for device_idx in range(len(devices)):
        for sketch_idx in range(num_unique_sketches):
            m.addConstr(gp.quicksum(data_plane_indicators[device_idx][sketch_idx]) <= 1)
    
    # constraint: for each device, the sum of memory of deployed sketches should not exceed the device's memory
    for device_idx in range(len(devices)):
        m.addConstr(gp.quicksum([data_plane_indicators[device_idx][sketch_idx][resource_idx] * memory[sketch_idx][resource_idx] for sketch_idx in range(num_unique_sketches) for resource_idx in range(num_profiles_per_sketch[sketch_idx])]) <= device_rams[device_idx])
    
    # constraint: if control_plane_indicator is 1, then the resource_configuration of the sketch deployed on the device should be greater than or equal to the minimum resource configuration
    for metric_idx in range(len(metrics)):
        metric = metrics[metric_idx]
        unique_sketch_idx = metric_unique_sketch_idx_map[metrics[metric_idx]]
        
        if run_strawman:
            min_resource_config = metric_min_sketch_resource_config[metric]
        else:
            min_resource_configs = metric_min_sketch_resource_config[metric]

            # indicator variables for row
            max_sketch_rows = len(min_resource_configs)
            row_indicators = [[m.addVar(lb=0, ub=1, vtype=GRB.BINARY) for _ in range(max_sketch_rows)] for _ in range(len(devices))]
       
        #print(metric, min_resource_config)
        for device_idx in range(len(devices)):
            m.addConstr(((control_plane_indicators[metric_idx][device_idx]) == 1) >> (gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx])) == 1))

            rows = gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * list(profiles[unique_sketches[unique_sketch_idx]][metric].keys())[resource_idx][1] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx]))
            widths = gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * list(profiles[unique_sketches[unique_sketch_idx]][metric].keys())[resource_idx][2] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx]))

            sketch_resource_configurations = profiles[unique_sketches[unique_sketch_idx]][metric].keys()
            #total_memory = gp.quicksum([data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * np.prod(resource_config) for resource_idx,resource_config in enumerate(sketch_resource_configurations)])
            #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, total_memory >= np.prod(metric_min_sketch_resource_config[metric]))

            if run_strawman:
                min_row = min_resource_config[1]
                min_width = min_resource_config[2]

                m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, rows == min_row)
                #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, widths == min_width)
                m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, widths >= min_width)
            else:
                #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, rows >= min_row)
                #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, widths >= min_width)

                #final_constr = None
                for idx, min_resource_config in enumerate(min_resource_configs):
                    min_row = min_resource_config[1]
                    min_width = min_resource_config[2]
                    m.addGenConstrIndicator(row_indicators[device_idx][idx], 1, widths >= min_width)
                    m.addGenConstrIndicator(row_indicators[device_idx][idx], 1, rows == min_row)
                    #if final_constr is None:
                    #    if run_strawman:
                    #        final_constr = gp.and_((rows == min_row), (widths == min_width))
                    #    else:
                    #        final_constr = gp.and_((rows >= min_row), (widths >= min_width))
                    #else:
                    #    if run_strawman:
                    #        final_constr = gp.or_(final_constr, gp.and_((rows == min_row), (widths == min_width)))
                    #    else:
                    #        final_constr = gp.or_(final_constr, gp.and_((rows >= min_row), (widths >= min_width)))
                #print(type(final_constr))
                #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, lhs=final_constr, rhs=1)
                #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, rows <= max_sketch_rows)
                m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, gp.quicksum(row_indicators[device_idx]) == 1)


    # constraint: for each OD pair, for each metric to be served, there must be a device on the OD pair that has deployed the query
    for od_pair in od_pairs:
        for metric in od_pair_metric_map[od_pair]:
            metric_idx = metrics.index(metric)
            m.addConstr(gp.quicksum([control_plane_indicators[metric_idx][device_idx] for device_idx in range(len(devices)) if device_idx in od_pair]) >= 1)
    
    # objective: minimize the sum of resource configurations of deployed sketches
    objective = gp.quicksum([data_plane_indicators[device_idx][sketch_idx][resource_idx] * memory[sketch_idx][resource_idx] for device_idx in range(len(devices)) for sketch_idx in range(num_unique_sketches) for resource_idx in range(num_profiles_per_sketch[sketch_idx])])
    m.setObjective(objective, GRB.MINIMIZE)
    m.optimize()

    if m.status == GRB.Status.OPTIMAL or m.SolCount > 0:
        print('Status: {}'.format(m.status))
        # convert gurobi variables to values
        data_plane_indicators = [[[data_plane_indicators[device_idx][sketch_idx][resource_idx].x for resource_idx in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)] for device_idx in range(len(devices))]
        memory = [[memory[sketch_idx][resource_idx].x for resource_idx in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)]
        control_plane_indicators = [[control_plane_indicators[metric_idx][device_idx].x for device_idx in range(len(devices))] for metric_idx in range(len(metrics))]

        #for device_idx in range(len(devices)):
        #    for sketch_idx in range(num_unique_sketches):
        #        for resource_idx in range(num_profiles_per_sketch[sketch_idx]):
        #            if data_plane_indicators[device_idx][sketch_idx][resource_idx] == 1:
        #                print('Device: {}, Sketch: {}, Resource: {}'.format(devices[device_idx], unique_sketches[sketch_idx], resource_idx))
        #                print('Memory: {}'.format(memory[sketch_idx][resource_idx]))
        #                some_sketch_metric = list(profiles[unique_sketches[sketch_idx]].keys())[0]
        #                print(list(profiles[unique_sketches[sketch_idx]][some_sketch_metric].keys())[resource_idx])
        #for metric_idx in range(len(metrics)):
        #    for device_idx in range(len(devices)):
        #        if control_plane_indicators[metric_idx][device_idx] == 1:
        #            print('Metric: {}, Device: {}'.format(metrics[metric_idx], devices[device_idx]))

        return m.objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators
    else:
        print('No solution found')
        return None

def get_subproblems_from_cluster_solution(cluster_ret, metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, od_pairs, devices, device_rams, cluster_device_map):
    cluster_objVal, cluster_num_profiles_per_sketch, cluster_data_plane_indicators, cluster_memory, cluster_control_plane_indicators = cluster_ret

    subproblems = []
    infos_for_collapsing = []

    for cluster_idx, cluster in enumerate(cluster_device_map):
        cluster_metrics = sorted([metric for metric_idx,metric in enumerate(metrics) if cluster_control_plane_indicators[metric_idx][cluster_idx] == 1])

        cluster_metric_sketch_map = {metric: metric_sketch_map[metric] for metric in cluster_metrics}
        cluster_metric_min_sketch_resource_config = {metric: metric_min_sketch_resource_config[metric] for metric in cluster_metrics}

        cluster_unique_sketches = list(set([sketch_idx for sketch_idx in range(len(cluster_data_plane_indicators[cluster_idx])) if sum(cluster_data_plane_indicators[cluster_idx][sketch_idx]) > 0]))
        cluster_unique_sketches = sorted([unique_sketches[sketch_idx] for sketch_idx in cluster_unique_sketches])

        # this contains od pairs consisting only of devices in this cluster
        cluster_od_pair = []
        for od_pair in od_pairs:
            filtered_od_pair = [device for device in od_pair if device in cluster]
            if len(filtered_od_pair) > 0:
                cluster_od_pair.append(tuple(filtered_od_pair))
        cluster_od_pair = sorted(list(set(cluster_od_pair)))

        cluster_device_rams = [device_rams[device] for device in cluster]
        #cluster_od_pair_metric_map = defaultdict(list)
        cluster_od_pair_metric_map = {od_pair: [] for od_pair in cluster_od_pair}
        for od_pair in cluster_od_pair:
            for metric in cluster_metrics:
                cluster_od_pair_metric_map[od_pair].append(metric)
            cluster_od_pair_metric_map[od_pair] = sorted(list(set(cluster_od_pair_metric_map[od_pair])))
        
        if len(cluster_metrics) == 0:
            continue
        
        subproblems.append((cluster_metrics, cluster_unique_sketches, cluster_metric_sketch_map, cluster_metric_min_sketch_resource_config, profiles, cluster_od_pair, devices, device_rams, cluster_od_pair_metric_map))
        infos_for_collapsing.append((cluster_metrics, cluster_unique_sketches))

    return subproblems, infos_for_collapsing

def collapse_subproblem_results(metrics, unique_sketches, metric_sketch_map, profiles, od_pairs, devices, device_rams, od_pair_metric_map, cluster_device_map, all_results, infos_for_collapsing):
    objVal = 0
    num_profiles_per_sketch = []
    for sketch in unique_sketches:
        this_sketch_metrics = list(profiles[sketch].keys())
        num_resource_configs = len(profiles[sketch][this_sketch_metrics[0]])
        num_profiles_per_sketch.append(num_resource_configs)

    data_plane_indicators = [[[0 for _ in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(len(unique_sketches))] for _ in range(len(devices))]
    control_plane_indicators = [[0 for _ in range(len(devices))] for _ in range(len(metrics))]
    memory = [[0 for _ in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(len(unique_sketches))]

    for idx, (ret, info_for_collapsing) in enumerate(zip(all_results, infos_for_collapsing)):
        assert(ret is not None)
        sub_cluster_metrics, sub_cluster_unique_sketches = info_for_collapsing
        sub_objVal, sub_num_profiles_per_sketch, sub_data_plane_indicators, sub_memory, sub_control_plane_indicators = ret

        objVal += sub_objVal

        for device_idx in cluster_device_map[idx]:
            for sketch_idx, sketch in enumerate(sub_cluster_unique_sketches):
                for resource_idx in range(sub_num_profiles_per_sketch[sketch_idx]):
                    data_plane_indicators[device_idx][unique_sketches.index(sketch)][resource_idx] = sub_data_plane_indicators[device_idx][sketch_idx][resource_idx]
                    memory[unique_sketches.index(sketch)][resource_idx] = sub_memory[sketch_idx][resource_idx]

        for metric_idx, metric in enumerate(sub_cluster_metrics):
            for device_idx in cluster_device_map[idx]:
                control_plane_indicators[metrics.index(metric)][device_idx] = sub_control_plane_indicators[metric_idx][device_idx]

    return objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators

# cluster_device_map: list of clusters, where each cluster is a list of devices
def sketchplan_placement(metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, od_pairs, devices, device_rams, od_pair_metric_map, cluster_device_map):
    device_cluster_map = {device: cluster_idx for cluster_idx, cluster in enumerate(cluster_device_map) for device in cluster}

    cluster_rams = [sum([device_rams[device_idx] for device_idx in cluster]) for cluster in cluster_device_map]
    
    cluster_od_pairs = []
    cluster_od_pair_metric_map = {}
    od_pair_cluster_od_pair_map = {}

    for od_pair in od_pairs:
        new_od_pair = [device_cluster_map[device] for device in od_pair]
        new_od_pair = sorted(list(set(new_od_pair)))
        od_pair_cluster_od_pair_map[tuple(od_pair)] = tuple(new_od_pair)
        cluster_od_pairs.append(new_od_pair)

    cluster_od_pairs = list(set([tuple(cluster_od_pair) for cluster_od_pair in cluster_od_pairs]))
    cluster_od_pair_metric_map = {cluster_od_pair: [] for cluster_od_pair in cluster_od_pairs}
    for od_pair, cluster_od_pair in od_pair_cluster_od_pair_map.items():
        for metric in od_pair_metric_map[od_pair]:
            cluster_od_pair_metric_map[cluster_od_pair].append(metric)
        cluster_od_pair_metric_map[cluster_od_pair] = sorted(list(set(cluster_od_pair_metric_map[cluster_od_pair])))

    cluster_ret = optimal_baseline_placement(metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, cluster_od_pairs, list(range(len(cluster_device_map))), cluster_rams, cluster_od_pair_metric_map)

    cluster_objVal, cluster_num_profiles_per_sketch, cluster_data_plane_indicators, cluster_memory, cluster_control_plane_indicators = cluster_ret
    #for device_idx in range(len(cluster_device_map)):
    #    for sketch_idx in range(len(unique_sketches)):
    #        for resource_idx in range(cluster_num_profiles_per_sketch[sketch_idx]):
    #            if cluster_data_plane_indicators[device_idx][sketch_idx][resource_idx] == 1:
    #                print('Cluster: {}, Sketch: {}, Resource: {}'.format(device_idx, unique_sketches[sketch_idx], resource_idx))
    #                print('Memory: {}'.format(cluster_memory[sketch_idx][resource_idx]))
    #                some_sketch_metric = list(profiles[unique_sketches[sketch_idx]].keys())[0]
    #                print(list(profiles[unique_sketches[sketch_idx]][some_sketch_metric].keys())[resource_idx])
    #for cluster_idx in range(len(cluster_device_map)):
    #    for metric_idx in range(len(metrics)):
    #        if cluster_control_plane_indicators[metric_idx][cluster_idx] == 1:
    #            print('Cluster: {}, Metric: {}'.format(cluster_idx, metrics[metric_idx]))

    if cluster_ret is None:
        print('No solution found for clusters')
        return None
   
    subproblems, infos_for_collapsing = get_subproblems_from_cluster_solution(cluster_ret, metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, od_pairs, devices, device_rams, cluster_device_map)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        all_futures = []
        all_results = []
        for subproblem in subproblems:
            all_futures.append(executor.submit(optimal_baseline_placement, *subproblem))

        for idx, future in enumerate(all_futures):
            ret = future.result()
            if ret is None:
                print('No solution found for subproblem {}'.format(idx))
                return None
            all_results.append(ret)

    final_ret = collapse_subproblem_results(metrics, unique_sketches, metric_sketch_map, profiles, od_pairs, devices, device_rams, od_pair_metric_map, cluster_device_map, all_results, infos_for_collapsing)

    return final_ret

def handle_sketch_metric_map(args, metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles):
    mode = args.mode
    # get list of sketches and their resource configurations
    unique_sketches = sorted(list(set(list(metric_to_sketch_map.values()))))
    local_metric_sketch_resource_configs = {} 

    discontinue = False
    for metric in METRICS:
        sketch = metric_to_sketch_map[metric]
        if len(candidate_metric_sketch_resource_configs[metric][sketch]) == 0:
            print('No resource configuration found for metric: {}, sketch: {}'.format(metric, sketch))
            discontinue = True
            break
        else:
            #local_metric_sketch_resource_configs[metric] = candidate_metric_sketch_resource_configs[metric][sketch][0]
            if not args.run_strawman:
                local_metric_sketch_resource_configs[metric] = []
                candidate_resource_configs = sorted(candidate_metric_sketch_resource_configs[metric][sketch])
                row_covered = {}
                for candidate_resource_config in candidate_resource_configs:
                    row = candidate_resource_config[1]
                    if row in row_covered:
                        continue
                    row_covered[row] = True
                    local_metric_sketch_resource_configs[metric].append(candidate_resource_config)
            else:
                local_metric_sketch_resource_configs[metric] = candidate_metric_sketch_resource_configs[metric][sketch][0]
                    
    
    if discontinue:
        return None

    # for now assume that each od pair has the same set of metrics
    od_pair_metric_map = {}
    for od_pair in OD_PAIRS:
        od_pair_metric_map[od_pair] = []
        for metric in METRICS:
            od_pair_metric_map[od_pair].append(metric)
    
    if mode == 'optimal_baseline':
        ret = optimal_baseline_placement(METRICS, unique_sketches, metric_to_sketch_map, local_metric_sketch_resource_configs, profiles, OD_PAIRS, DEVICES, DEVICE_RAMS, od_pair_metric_map, args.run_strawman, time_limit=args.optimal_baseline_time_limit)
    elif mode == 'sketchplan':
        ret = sketchplan_placement(METRICS, unique_sketches, metric_to_sketch_map, local_metric_sketch_resource_configs, profiles, OD_PAIRS, DEVICES, DEVICE_RAMS, od_pair_metric_map, CLUSTER_DEVICE_MAP)
    else:
        assert(False)

    if ret is None:
        return None

    objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators = ret
    return objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs

def set_globals(od_pairs, cluster_device_map):
    global NUM_OD_PAIRS, ALL_DEVICES, NUM_DEVICES, DEVICES, DEVICE_RAMS, NUM_CLUSTERS, CLUSTER_DEVICE_MAP
    
    NUM_OD_PAIRS = len(OD_PAIRS)
    ALL_DEVICES = []
    for od_pair in OD_PAIRS:
        ALL_DEVICES += od_pair
    ALL_DEVICES = list(set(ALL_DEVICES))
    NUM_DEVICES = max(ALL_DEVICES) + 1
    DEVICES = ['device{}'.format(i) for i in range(0, NUM_DEVICES)]
    DEVICE_RAMS = [DEVICE_SRAM for i in range(0, NUM_DEVICES)]
    NUM_CLUSTERS = len(CLUSTER_DEVICE_MAP)

def main(args):
    global OD_PAIRS, CLUSTER_DEVICE_MAP, METRICS, DEVICE_SRAM, DEVICE_RAMS, SKETCHES, ERROR_BOUNDS
    if args.od_pkl:
        OD_PAIRS = pickle.load(open(args.od_pkl, 'rb'))
        OD_PAIRS = [tuple(od_pair) for od_pair in OD_PAIRS]
        set_globals(OD_PAIRS, CLUSTER_DEVICE_MAP)
    if args.cluster_device_map_pkl:
        CLUSTER_DEVICE_MAP = pickle.load(open(args.cluster_device_map_pkl, 'rb'))
        set_globals(OD_PAIRS, CLUSTER_DEVICE_MAP)
    METRICS = args.metric.split(',')
    DEVICE_SRAM = 2**args.sram
    DEVICE_RAMS = [DEVICE_SRAM for i in range(0, NUM_DEVICES)]

    error_bound_tokens = args.error_bounds_string.split('_')
    assert(len(error_bound_tokens) % 2 == 0)
    ERROR_BOUNDS = {}
    for i in range(0, len(error_bound_tokens), 2):
        metric = error_bound_tokens[i]
        error_bound = int(error_bound_tokens[i + 1])
        ERROR_BOUNDS[metric] = error_bound

    if args.run_strawman:
        metric_to_sketch_maps, metric_to_available_sketches_map = get_metric_to_sketch_maps_strawman(args.strawman_json_file, args.strawman_name, args.strawman_seed)
    else:
        metric_to_sketch_maps, metric_to_available_sketches_map = get_metric_to_sketch_maps()

    #profiles = utils.read_and_process_profiles(profiles_path, metric_max_json_file, 'median')
    profiles = utils.read_and_process_profiles(profiles_path, None, 'median', start_idx=0, end_idx=18)
    profiles = flatten_profiles(profiles)

    best_resource = float('inf')
    best_solution = None

    candidate_metric_sketch_resource_configs = {metric: {sketch: [] for sketch in SKETCHES} for metric in METRICS}
    for metric in METRICS:
        for sketch in metric_to_available_sketches_map[metric]:
            # find the smallest resource_configuration which satisfies the accuracy bound
            for resource_config in profiles[sketch][metric].keys():
                if profiles[sketch][metric][resource_config] <= ERROR_BOUNDS[metric]:
                    candidate_metric_sketch_resource_configs[metric][sketch].append(resource_config)
            candidate_metric_sketch_resource_configs[metric][sketch] = sorted(candidate_metric_sketch_resource_configs[metric][sketch], key=lambda x: np.prod(x))

            print('Candidates for ', metric, sketch)
            print(candidate_metric_sketch_resource_configs[metric][sketch])

            if not args.run_strawman:
                # if strawman, keep all
                # else, just keep lowest
                candidate_metric_sketch_resource_configs[metric][sketch] = candidate_metric_sketch_resource_configs[metric][sketch][:1]

    if args.run_strawman and args.strawman_resource_policy_agnostic:
        # rewrite candidate_metric_sketch_resource_configs to be based on strawman uniform/proportional
        assert(len(metric_to_sketch_maps) == 1)
        unique_sketches = sorted(list(set(list(metric_to_sketch_maps[0].values()))))
        sketch_frequency = {sketch: 0 for sketch in unique_sketches}
        for metric in METRICS:
            sketch = metric_to_sketch_maps[0][metric]
            sketch_frequency[sketch] += 1
        resource_bound_per_sketch = {}
        for sketch in unique_sketches:
            if 'equal' in args.strawman_name:
                resource_bound_per_sketch[sketch] = DEVICE_SRAM / len(unique_sketches)
            elif 'proportional' in args.strawman_name:
                resource_bound_per_sketch[sketch] = DEVICE_SRAM * sketch_frequency[sketch] / len(METRICS)
            else:
                raise NotImplementedError(args.strawman_name)

        strawman_resource_configs = {}
        for sketch in unique_sketches:
            resource_allocation = utils.get_strawman_resource_allocation_from_resource_bound(resource_bound_per_sketch[sketch], sketch, False).get_resource_allocation()
            strawman_resource_configs[sketch] = (resource_allocation['level'], resource_allocation['row'], resource_allocation['width'])

        print(candidate_metric_sketch_resource_configs)
        print(strawman_resource_configs)

        for metric in METRICS:
            for sketch in candidate_metric_sketch_resource_configs[metric].keys():
                if sketch not in strawman_resource_configs or len(candidate_metric_sketch_resource_configs[metric][sketch]) == 0:
                    candidate_metric_sketch_resource_configs[metric][sketch] = []
                else:
                    #candidate_metric_sketch_resource_configs[metric][sketch] = list(set.intersection(set([strawman_resource_configs[sketch]]), set(candidate_metric_sketch_resource_configs[metric][sketch])))
                    assigned = False
                    for candidate in candidate_metric_sketch_resource_configs[metric][sketch]:
                        #if strawman_resource_configs[sketch] >= candidate:
                        if all(s >= c for s,c in zip(strawman_resource_configs[sketch], candidate)):
                            print('Assigning {} {} {}'.format(metric, sketch, strawman_resource_configs[sketch]))
                            candidate_metric_sketch_resource_configs[metric][sketch] = [strawman_resource_configs[sketch]]
                            assigned = True
                            break
                    if not assigned:
                        candidate_metric_sketch_resource_configs[metric][sketch] = []

    if args.run_strawman:
        print(candidate_metric_sketch_resource_configs)

    start = time.time()

    all_results = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for idx, metric_to_sketch_map in enumerate(metric_to_sketch_maps):
            all_results.append(executor.submit(handle_sketch_metric_map, args, metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles))
            print('Submitted job {}'.format(idx))

        for future_idx, future in enumerate(concurrent.futures.as_completed(all_results)):
            print('Processing future {}/{}'.format(future_idx, len(all_results)))
            ret = future.result()
            if ret is not None:
                objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs = ret
                if objVal < best_resource:
                    best_resource = objVal
                    best_solution = num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs

    end = time.time()

    if best_solution is not None:
        if args.output_pkl:
            os.makedirs(os.path.dirname(args.output_pkl), exist_ok=True)
            with open(args.output_pkl, 'wb') as fout:
                pickle.dump((best_resource, best_solution), fout)

        num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs = best_solution
        print('Best solution:')
        print('Objective value: {}'.format(best_resource))
        print('Metric to sketch map: {}'.format(metric_to_sketch_map))
        print('Metric to sketch map idx: {}'.format(metric_to_sketch_maps.index(metric_to_sketch_map)))
        print(local_metric_sketch_resource_configs)

        # print data plane indicators from model m
        for device_idx in range(len(DEVICES)):
            for sketch_idx in range(len(unique_sketches)):
                for resource_idx in range(num_profiles_per_sketch[sketch_idx]):
                    if data_plane_indicators[device_idx][sketch_idx][resource_idx] == 1:
                        print('Device: {}, Sketch: {}, Resource: {}'.format(DEVICES[device_idx], unique_sketches[sketch_idx], resource_idx))
                        print('Memory: {}'.format(memory[sketch_idx][resource_idx]))
                        some_sketch_metric = list(profiles[unique_sketches[sketch_idx]].keys())[0]
                        print(list(profiles[unique_sketches[sketch_idx]][some_sketch_metric].keys())[resource_idx])

        # print control plane indicators from model m
        for metric_idx in range(len(METRICS)):
            sketch = metric_to_sketch_map[METRICS[metric_idx]]
            sketch_idx = unique_sketches.index(sketch)
            for device_idx in range(len(DEVICES)):
                if control_plane_indicators[metric_idx][device_idx] == 1:
                    print('Metric: {}, Device: {}'.format(METRICS[metric_idx], DEVICES[device_idx]))
                    for resource_idx in range(num_profiles_per_sketch[sketch_idx]):
                        if data_plane_indicators[device_idx][sketch_idx][resource_idx] == 1:
                            print('Resource: {}'.format(resource_idx))
                            print(list(profiles[sketch][METRICS[metric_idx]].keys())[resource_idx])
                            print('Error: {}'.format(profiles[sketch][METRICS[metric_idx]][list(profiles[sketch][METRICS[metric_idx]].keys())[resource_idx]]))
    print('Time taken: {}'.format(end - start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', required=True, type=str, help='Mode')
    parser.add_argument('--verbose', action='store_true', help='Verbose')
    parser.add_argument('--run_strawman', action='store_true', help='Run strawman')
    parser.add_argument('--strawman_json_file', type=str, help='Strawman json file')
    parser.add_argument('--strawman_name', type=str, help='Strawman name in the format "Random:equal"')
    parser.add_argument('--strawman_seed', type=int)
    parser.add_argument('--od_pkl', required=False)
    parser.add_argument('--cluster_device_map_pkl', required=False)
    parser.add_argument('--optimal_baseline_time_limit', type=int, default=0)
    parser.add_argument('--metric', required=True)
    parser.add_argument('--output_pkl', required=False)
    parser.add_argument('--sram', type=int, required=True)
    parser.add_argument('--strawman_resource_policy_agnostic', action='store_true', help='Strawman resources are allocated based on strawman uniform/proportional, not based on accuracy bound policy')
    parser.add_argument('--error_bounds_string', type=str, required=True)
    args = parser.parse_args()
    main(args)
