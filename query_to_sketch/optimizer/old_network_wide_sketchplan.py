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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main_constants import *
import utils

path = os.path.join(ROOT_DIR, 'sketch_metric_coverage','actual.pkl')
profiles_path = PROFILES_PATH
metric_max_json_file = os.path.join(PROFILES_PATH, 'metric_max.json')

with open("od_pairs.pkl", 'rb') as fin:
        OD_PAIRS = pickle.load(fin)

NUM_OD_PAIRS = len(OD_PAIRS)
ALL_DEVICES = []
for od_pair in OD_PAIRS:
    ALL_DEVICES += od_pair
ALL_DEVICES = list(set(ALL_DEVICES))
NUM_DEVICES = max(ALL_DEVICES)

DEVICES = ['device{}'.format(i) for i in range(0, NUM_DEVICES)]
DEVICE_RAMS = [2**19 for i in range(0, NUM_DEVICES)]

METRICS = ['hh', 'ent', 'cardinality', 'cd', 'fsd']
ERROR_BOUNDS = {
    'hh': 5,
    'ent': 5,
    'cardinality': 3,
    'cd': 5,
    'fsd': 50,
}
SKETCHES = ['cm', 'cs', 'hll', 'lc', 'll', 'mrac', 'mrb', 'univmon']

def get_metric_to_sketch_maps():
    metric_to_sketch_data = None
    with open(path, 'rb') as fin:
        metric_to_sketch_data = pickle.load(fin)

    metric_to_available_sketches_map = {}

    for metric in METRICS:
        metric_to_available_sketches_map[metric] = list(set(SKETCHES) & set(metric_to_sketch_data['metric_to_sketch'][metric]))

    iterables = [[(m, v) for v in metric_to_available_sketches_map[m]] for m in metric_to_available_sketches_map.keys()]
    product = itertools.product(*iterables)

    result = []
    for p in product:
        result.append({m:s for (m, s) in p})
    return result, metric_to_available_sketches_map

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

def optimize_sketch_placement_on_cluster_device(metrics, unique_sketches, metric_sketch_map, metric_min_sketch_resource_config, profiles, od_pairs, devices, device_rams, od_pair_metric_map):
    m = gp.Model()
    m.params.OutputFlag = 0

    # for now assume that each od pair has the same set of metrics
    for od_pair in od_pairs:
        od_pair = tuple(od_pair)
        for metric in metrics:
            od_pair_metric_map[od_pair].append(metric)

    num_unique_sketches = len(unique_sketches)
    num_profiles_per_sketch = []
    for sketch in unique_sketches:
        this_sketch_metrics = list(profiles[sketch].keys())
        num_resource_configs = len(profiles[sketch][this_sketch_metrics[0]])
        num_profiles_per_sketch.append(num_resource_configs)

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
        min_resource_config = metric_min_sketch_resource_config[metric]
       
        #print(metric, min_resource_config)
        min_row = min_resource_config[1]
        min_width = min_resource_config[2]

        for device_idx in range(len(devices)):
            m.addConstr(((control_plane_indicators[metric_idx][device_idx]) == 1) >> (gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx])) == 1))

            rows = gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * list(profiles[unique_sketches[unique_sketch_idx]][metric].keys())[resource_idx][1] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx]))
            widths = gp.quicksum(data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * list(profiles[unique_sketches[unique_sketch_idx]][metric].keys())[resource_idx][2] for resource_idx in range(num_profiles_per_sketch[unique_sketch_idx]))

            sketch_resource_configurations = profiles[unique_sketches[unique_sketch_idx]][metric].keys()
            #total_memory = gp.quicksum([data_plane_indicators[device_idx][unique_sketch_idx][resource_idx] * np.prod(resource_config) for resource_idx,resource_config in enumerate(sketch_resource_configurations)])
            #m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, total_memory >= np.prod(metric_min_sketch_resource_config[metric]))
            m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, rows >= min_row)
            m.addGenConstrIndicator(control_plane_indicators[metric_idx][device_idx], 1, widths >= min_width)

    # constraint: for each OD pair, for each metric to be served, there must be a device on the OD pair that has deployed the query
    for od_pair in od_pairs:
        od_pair = tuple(od_pair)
        for metric in od_pair_metric_map[od_pair]:
            metric_idx = metrics.index(metric)
            m.addConstr(gp.quicksum([control_plane_indicators[metric_idx][device_idx] for device_idx in range(len(devices)) if device_idx in od_pair]) >= 1)
    
    # objective: minimize the sum of resource configurations of deployed sketches
    objective = gp.quicksum([data_plane_indicators[device_idx][sketch_idx][resource_idx] * memory[sketch_idx][resource_idx] for device_idx in range(len(devices)) for sketch_idx in range(num_unique_sketches) for resource_idx in range(num_profiles_per_sketch[sketch_idx])])
    m.setObjective(objective, GRB.MINIMIZE)
    m.optimize()

    if m.status == GRB.Status.OPTIMAL:
        # convert gurobi variables to values
        data_plane_indicators = [[[data_plane_indicators[device_idx][sketch_idx][resource_idx].x for resource_idx in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)] for device_idx in range(len(devices))]
        memory = [[memory[sketch_idx][resource_idx].x for resource_idx in range(num_profiles_per_sketch[sketch_idx])] for sketch_idx in range(num_unique_sketches)]
        control_plane_indicators = [[control_plane_indicators[metric_idx][device_idx].x for device_idx in range(len(devices))] for metric_idx in range(len(metrics))]
        return m, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators
    else:
        print('No solution found')
        return None

def handle_sketch_metric_map(metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles):
    # get list of sketches and their resource configurations
    unique_sketches = sorted(list(set(list(metric_to_sketch_map.values()))))
    local_metric_sketch_resource_configs = {} 

    discontinue = False
    for metric in METRICS:
        sketch = metric_to_sketch_map[metric]
        if len(candidate_metric_sketch_resource_configs[metric][sketch]) == 0:
            #print('No resource configuration found for metric: {}, sketch: {}'.format(metric, sketch))
            discontinue = True
            break
        else:
            local_metric_sketch_resource_configs[metric] = candidate_metric_sketch_resource_configs[metric][sketch][0]
    
    if discontinue:
        return None

    od_pair_metric_map = defaultdict(list)
    ret = optimize_sketch_placement_on_cluster_device(METRICS, unique_sketches, metric_to_sketch_map, local_metric_sketch_resource_configs, profiles, OD_PAIRS, DEVICES, DEVICE_RAMS, od_pair_metric_map)

    if ret is None:
        return None

    m, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators = ret

    return m.objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs

    #if m.objVal < best_resource:
    #    best_resource = m.objVal
    #    best_solution = m, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs

def main(args):
    metric_to_sketch_maps, metric_to_available_sketches_map = get_metric_to_sketch_maps() 
    profiles = utils.read_and_process_profiles(profiles_path, metric_max_json_file, 'median')
    profiles = flatten_profiles(profiles)

    best_resource = float('inf')
    best_solution = None

    candidate_metric_sketch_resource_configs = {metric: {sketch: [] for sketch in SKETCHES} for metric in METRICS}
    for metric in METRICS:
        for sketch in metric_to_available_sketches_map[metric]:
            # find smallest resource_configuration which satisfies the accuracy bound
            for resource_config in profiles[sketch][metric].keys():
                if profiles[sketch][metric][resource_config] <= ERROR_BOUNDS[metric]:
                    candidate_metric_sketch_resource_configs[metric][sketch].append(resource_config)
            candidate_metric_sketch_resource_configs[metric][sketch] = sorted(candidate_metric_sketch_resource_configs[metric][sketch], key=lambda x: np.prod(x))[:1]

    print(candidate_metric_sketch_resource_configs)

    start = time.time()

    all_results = [None for _ in range(0, len(metric_to_sketch_maps))]
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=14) as executor:
        for idx, metric_to_sketch_map in enumerate(metric_to_sketch_maps):
            all_results[idx] = executor.submit(handle_sketch_metric_map, metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles)
            #all_results[idx] = executor.apply_async(handle_sketch_metric_map, metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles)
            #all_results[idx] = executor.apply_async(test_handle, (metric_to_sketch_map, candidate_metric_sketch_resource_configs, profiles))
            print('Submitted job {}'.format(idx))

        #for result in all_results:
        #    if result.ready():
        #        print('Processing future')
        #        ret = result.get()
        #        if ret is not None:
        #            #m, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs = ret
        #            if m.objVal < best_resource:
        #                best_resource = m.objVal
        #                best_solution = m, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs
        for future in concurrent.futures.as_completed(all_results):
            print('Processing future')
            ret = future.result()
            if ret is not None:
                objVal, num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs = ret
                if objVal < best_resource:
                    best_resource = objVal
                    best_solution = num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs

    end = time.time()

    if best_solution is not None:
        num_profiles_per_sketch, data_plane_indicators, memory, control_plane_indicators, metric_to_sketch_map, unique_sketches, local_metric_sketch_resource_configs = best_solution
        print('Best solution:')
        print('Objective value: {}'.format(best_resource))
        print('Metric to sketch map: {}'.format(metric_to_sketch_map))
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
    args = parser.parse_args()
    main(args)
