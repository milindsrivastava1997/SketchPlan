import os

SKETCH_HOME = os.path.expandvars('$sketch_home')
ROOT_DIR = os.path.join(SKETCH_HOME, 'query_to_sketch')
#PROFILES_PATH = os.path.join(ROOT_DIR, 'profiler', 'actual_profiles')
PROFILES_PATH = os.path.join(ROOT_DIR, 'profiler', 'actual_profiles_lower_memory')
#PROFILES_PATH = os.path.join(ROOT_DIR, 'profiler', 'actual_profiles_lower_memory_new_fsd_2')
#PROFILES_PATH = os.path.join(ROOT_DIR, 'profiler', 'profiles_caida_2018')
METRIC_MAX_JSON_FILE = None

pretty_sketch_map = {
    'cm': 'Count-min Sketch',
    'cs': 'Count Sketch',
    'hhh': 'HHH',
    'rhhh': 'RHHH',
    'univmon': 'Univmon',
    'fcm': 'FCM',
    'lc': 'Linear Counting',
    'pcsa': 'PCSA',
    'mrb': 'Multi-resolution Bitmap',
    'll': 'Loglog',
    'hll': 'Hyperloglog',
    'mrac': 'MRAC',
    'bf': 'BloomFilter'
}

pretty_metric_map = {
    'hh': 'heavy hitter',
    'cd': 'change detection',
    'ent': 'entropy',
    'cardinality': 'cardinality',
    'fsd': 'flow size distribution',
    'membership': 'membership',
}

DATAPLANE_EPOCH_MAP = {
    'sw': '30',
    'dpdk': '60',
    'xdp': '60'
}

DATAPLANE_CP_FILE_NAME_MAP = {
    'sw': 'data.pkl.class',
    'dpdk': 'data.pkl.class',
    'xdp': 'data.pkl.class'
}

total_sketches = list(pretty_sketch_map.keys())
total_metrics = list(pretty_metric_map.keys())
total_resources = ['hash_unit', 'salu', 'sram', 'columns']
