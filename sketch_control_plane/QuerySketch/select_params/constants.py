import os

ROOT_DIR = os.path.expandvars('$sketch_home')
#ROOT_DIR = os.path.join('/', 'data', 'sketch_home')
FLOWKEY = 'dstIP,dstPort'
#EPOCH_LENGTH = 60
#EPOCH_LENGTH = 30
#EPOCH_NUM = 60 // EPOCH_LENGTH

#DP_SKETCH_MAP = {
#    'dpdk': ['cm', 'cs', 'univmon'],
#    'sw': ['univmon'],
#    'xdp': ['cm', 'cs'],
#    'new_xdp': ['cm', 'cs'],
#    'new_dpdk': ['cm', 'cs']
#}

DP_HASH_MAP = {
    'dpdk': 'xxhash_hash',
    'xdp': 'xxhash_hash',
    'new_xdp': 'xxhash_hash',
    'new_dpdk': 'xxhash_hash',
    'sw': 'crc_hash',
    'smartnic': 'anup_hash'
}

DP_EPOCH_LENGTH_MAP = {
    'dpdk': 60,
    'xdp': 60,
    'new_xdp': 60,
    'new_dpdk': 60,
    'sw': 30
}

#PCAPS = []
#PCAPS.append('equinix-nyc.dirA.20180517-130900.UTC.anon.pcap')
##PCAPS.append('equinix-nyc.dirA.20180517-131000.UTC.anon.pcap')
##PCAPS.append('equinix-nyc.dirA.20180517-131100.UTC.anon.pcap')
#
#SKETCH_PARAMS_MAP = {}
##SKETCH_PARAMS_MAP['cm'] = [(2, 2048, 1), (2, 4096, 1)]
##SKETCH_PARAMS_MAP['cs'] = [(1, 16384, 1), (2, 2048, 1), (2, 4096, 1)]
#SKETCH_PARAMS_MAP['cs'] = [(3, 65536, 1)]
#SKETCH_PARAMS_MAP['cm'] = [(3, 65536, 1)]
##SKETCH_PARAMS_MAP['univmon'] = [(3, 65536, 6)]
##SKETCH_PARAMS_MAP['univmon'] = [(2, 256, 16), (2, 512, 16), (3, 256, 16)]
##SKETCH_PARAMS_MAP['univmon'] = [(4, 2048, 16)]
#SKETCH_PARAMS_MAP['univmon'] = [(2, 8192, 16)]
#SKETCH_PARAMS_MAP['univmon'] = [(5, 8192, 16)]
