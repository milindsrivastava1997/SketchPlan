import argparse
import gurobipy as gp
from gurobipy import GRB

import sys
import pickle
import itertools
import os
import json
import numpy as np
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main_constants import *
import utils

path = os.path.join(ROOT_DIR, 'sketch_metric_coverage','actual.pkl')
profiles_path = PROFILES_PATH
metric_max_json_file = os.path.join(PROFILES_PATH, 'metric_max.json')

ALL_METRICS = ['hh', 'ent', 'cardinality', 'cd', 'fsd']

def main(args, metrics):

    metric_to_sketch_data = None
    with open(path, 'rb') as fin:
        metric_to_sketch_data = pickle.load(fin)

    start = time.time()

    metric_to_available_sketches_map = {}
    sketches = ['cm', 'cs', 'hll', 'lc', 'll', 'mrac', 'mrb', 'univmon']
    print(metrics)
    #sram = 1048576
    sram = args.total_sram

    for metric in metrics:
        metric_to_available_sketches_map[metric] = list(set(sketches) & set(metric_to_sketch_data['metric_to_sketch'][metric]))

    iterables = [[(m, v) for v in metric_to_available_sketches_map[m]] for m in metric_to_available_sketches_map.keys()]
    product = itertools.product(*iterables)

    result = []
    for p in product:
        result.append({m:s for (m, s) in p})

    prof = utils.read_and_process_profiles(profiles_path, metric_max_json_file, 'median', None, None)

    error_total = []
    memory_config = []
    error_config = []
    
    for z in range(len(result)):
        m = gp.Model()
        m.Params.OutputFlag = 0
        #find a better way to do this
        
        curr_deployment = gp.tupledict(result[z])
        # map from sketch to metrics
        curr_deployment_rev = {}
        for key, value in curr_deployment.items():
            if value not in curr_deployment_rev:
                curr_deployment_rev[value] = [key]
            else:
                curr_deployment_rev[value].append(key)
        
        # number of resource configurations for each sketch
        num_ind = []
        for i in curr_deployment_rev:
            ct = 0
            for j in prof[i][curr_deployment_rev[i][0]]:
                for k in prof[i][curr_deployment_rev[i][0]][j]:
                    if ct==0:
                        num_ind.append(len(prof[i][curr_deployment_rev[i][0]]) * len(prof[i][curr_deployment_rev[i][0]][j]) * len(prof[i][curr_deployment_rev[i][0]][j][k]))
                        ct = 1
                        
        mem = {}
        err = {}
        val = {}
        for i in curr_deployment:
            ct = 0
            mem_temp = []
            err_temp = []
            val_temp = []
            for j in prof[curr_deployment[i]][i]:
                for k in prof[curr_deployment[i]][i][j]:
                    for l in prof[curr_deployment[i]][i][j][k]:
                        mem_temp.append(j * k * l)
                        val_temp.append([j,k,l])
                        err_temp.append(prof[curr_deployment[i]][i][j][k][l])
            mem[i] = (mem_temp)
            err[i] = (err_temp)
            val[i] = (val_temp)
        
        # Create a 2D array of decision variables
        var = [[m.addVar(lb=0, ub=1, vtype=GRB.BINARY, name=f'x_{i}_{j}') for j in range(num_ind[i])] for i in range(len(num_ind))]
        for i in range(len(num_ind)):
            m.addConstr(gp.quicksum(var[i]) == 1, f'array_sum_{i}')
        
        errors = m.addVars(len(curr_deployment),vtype=GRB.CONTINUOUS, name="a")
        memory = m.addVars(len(num_ind),vtype=GRB.INTEGER, name="b")
        ct_1=0
        for i in curr_deployment_rev:
            for j in curr_deployment_rev[i]:
                for x in range(num_ind[list(curr_deployment_rev.keys()).index(i)]):
                    m.addConstr((var[list(curr_deployment_rev.keys()).index(i)][x] == 1) >> (errors[ct_1] == err[j][x]), name="indicator_constr1")
                    m.addConstr((var[list(curr_deployment_rev.keys()).index(i)][x] == 1) >> (memory[list(curr_deployment_rev.keys()).index(i)] == mem[j][x]), name="indicator_constr2")
                ct_1 = ct_1 + 1 
                
        m.addConstr(memory.sum() <= sram)
        m.setObjective(errors.sum(), GRB.MINIMIZE)
        
        m.optimize()
        
        if m.Status == GRB.OPTIMAL:
            mem_dict = {}
            error_dict = {}
            deploy_dict = []
            ct_1 = 0
            s1 = m.getAttr("X",errors)
            s2 = m.getAttr("X",memory)
            for i in curr_deployment_rev:
                for j in curr_deployment_rev[i]:
                    for x in range(num_ind[list(curr_deployment_rev.keys()).index(i)]):
                        if(var[list(curr_deployment_rev.keys()).index(i)][x].X == 1):
                            mem_dict[j,i] = val[j][x]
                            error_dict[j,i] = s1[ct_1]
                    ct_1 = ct_1 + 1
            memory_config.append(mem_dict)
            error_config.append(error_dict)
            error_total.append(sum(error_dict.values()))

    error_total = np.array(error_total)
    index = np.argmin(error_total)
    final_deployment = memory_config[index]

    end = time.time()

    print(f'Time taken: {end-start}')

    output = {}
    output['solutions'] = [{'algo': keys[1], 'metric': keys[0], 'level': final_deployment[keys][0], 'row': final_deployment[keys][1], 'width': final_deployment[keys][2]} for keys in final_deployment]

    print(output)

    with open("sample.json", "w") as outfile: 
        json.dump(output, outfile, indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--total_sram', type=int, required=True)
    args = parser.parse_args()

    metric_params = list(itertools.combinations(ALL_METRICS, 5)) + list(itertools.combinations(ALL_METRICS, 4))
    for metric_param in metric_params:
        main(args, metric_param)
