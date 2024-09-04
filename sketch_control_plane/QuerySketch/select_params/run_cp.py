import os
import pickle
import argparse

from python_lib.run_parallel_helper import ParallelRunHelper
from sketch_control_plane.common import metric_classes

from sketch import cm, cs, univmon, ll, hll, lc, mrac, mrb
import constants

def run_parallel_helper_function(sketch, input_path, sketch_args, output_path, dry_run):
    ret_list = []
    epochs = sorted(os.listdir(input_path))
    for epoch in epochs:
        sketch_args[1] = os.path.join(input_path, epoch)
        tuple_sketch_args = tuple(sketch_args)

        for arg in tuple_sketch_args:
            assert(arg is not None)

        if sketch == 'cm':
            ret = cm.cm_main(*tuple_sketch_args)
        elif sketch == 'cs':
            ret = cs.cs_main(*tuple_sketch_args)
        elif sketch == 'univmon':
            ret = univmon.univmon_main(*tuple_sketch_args)
        elif sketch == 'll':
            ret = ll.ll_main(*tuple_sketch_args)
        elif sketch == 'hll':
            ret = hll.hll_main(*tuple_sketch_args)
        elif sketch == 'lc':
            ret = lc.lc_main(*tuple_sketch_args)
        elif sketch == 'mrac':
            ret = mrac.mrac_main(*tuple_sketch_args)
        elif sketch == 'mrb':
            ret = mrb.mrb_main(*tuple_sketch_args)
        else:
            assert(False)

        ret_list.append(ret)

    if not dry_run:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as fout:
            pickle.dump(ret_list, fout)

def main(args):
    args_list = []
    input_dir_name = 'result_{}_dp'.format(args.dataplane)
    output_dir_name = 'result_{}_cp'.format(args.dataplane)

    if args.input_dir_suffix:
        input_dir_name += '_' + args.input_dir_suffix
        output_dir_name += '_' + args.input_dir_suffix

    input_root_dir = os.path.join(constants.ROOT_DIR, input_dir_name, args.project_name)
    output_root_dir = os.path.join(constants.ROOT_DIR, output_dir_name, args.project_name)
    
    sketches = sorted(os.listdir(input_root_dir))
    for sketch in sketches:
        sketch_dir = os.path.join(input_root_dir, sketch)
        pcaps = sorted(os.listdir(sketch_dir))
        for pcap in pcaps:
            pcap_dir = os.path.join(sketch_dir, pcap)
            flowkeys = sorted(os.listdir(pcap_dir))
            for flowkey in flowkeys:
                flowkey_dir = os.path.join(pcap_dir, flowkey)
                configs = sorted(os.listdir(flowkey_dir))
                for config in configs:
                    config_dir = os.path.join(flowkey_dir, config)
                    tokens = config.split('_')
                    rows = int(tokens[1])
                    width = int(tokens[3])
                    levels = int(tokens[5])

                    input_path = config_dir
                    output_path = os.path.join(output_root_dir, sketch, pcap, flowkey, config, 'data.pkl')
                    if args.file_suffix:
                        output_path += '.' + args.file_suffix
                    
                    sketch_args = [sketch, None, rows, width, levels, rows, args.dataplane, args.topk]

                    if sketch == 'cm' or sketch == 'cs' or sketch == 'mrac':
                        sketch_args.append(20) # bin_size

                    args_list.append((sketch, input_path, sketch_args, output_path, args.dry_run))

                    #epochs = sorted(os.listdir(config_dir))
                    #for epoch in epochs:
                    #    epoch_dir = os.path.join(config_dir, epoch)
                    #    
                    #    #print(sketch, pcap, flowkey, config, epoch)

                    #    input_path = epoch_dir
                    #    output_path = os.path.join(output_root_dir, sketch, pcap, flowkey, config, epoch, 'data.pkl')
                    #    if args.file_suffix:
                    #        output_path += '.' + args.file_suffix

                    #    sketch_args = (sketch, input_path, rows, width, levels, rows, args.dataplane, args.topk)

                    #    args_list.append((sketch, sketch_args, output_path, args.dry_run))
   
    prh = ParallelRunHelper(args.max_processes)
    for process_args in args_list:
        prh.call(run_parallel_helper_function, process_args)
    prh.join()
                        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataplane', required=True)
    parser.add_argument('--input_dir_suffix')
    parser.add_argument('--max_processes', type=int, required=True)
    parser.add_argument('--file_suffix', default='class')
    parser.add_argument('--topk', type=int, default=100)
    parser.add_argument('--project_name', default='QuerySketch')
    parser.add_argument('--dry_run', action='store_true')
    args = parser.parse_args()
    main(args)
