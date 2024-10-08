import os
import sys
import binascii
import crcmod
import xxhash

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from seeds import xxhash_seeds

crc_dict = {}

def universal_hash(flowkey, params):
    src_addr = 0
    src_port = 0
    dst_addr = 0
    dst_port = 0
    proto = 0
    if "srcIP" in flowkey.key:
        src_addr = flowkey.src_addr
    if "srcPort" in flowkey.key:
        src_port = flowkey.src_port
    if "dstIP" in flowkey.key:
        dst_addr = flowkey.dst_addr
    if "dstPort" in flowkey.key:
        dst_port = flowkey.dst_port
    if "proto" in flowkey.key:
        proto = flowkey.proto

    xored_flowkey = src_addr ^ src_port ^ dst_addr ^ dst_port ^ proto

    a = params[0]
    b = params[1]
    # print(xored_flowkey, a, b)
    return (a * xored_flowkey + b) % 39916801

def anup_hash(flowkey, params=None):
    if flowkey.key != "dstIP,dstPort":
        print("Don't know how to calculate anup_hash for flowkey:", flowkey.key)
        assert(False)

    if params['row'] % 4 == 0:
        return flowkey.dst_addr
    elif params['row'] % 4 == 1:
        return flowkey.dst_port
    elif params['row'] % 4 == 2:
        return flowkey.dst_addr & flowkey.dst_port
    else:
        return flowkey.dst_addr | flowkey.dst_port

def crc_hash(flowkey, params):
    poly = params[2]
    init = params[3]
    xout = params[4]
    # print(key)
    # key_byte = ((key)).to_bytes(8, byteorder='big')
    key_byte = b''

    
    # if "srcIP" in flowkey.key:
    #     key_byte += (flowkey.src_addr).to_bytes(4, byteorder='big')
    # if "dstIP" in flowkey.key:
    #     key_byte += (flowkey.dst_addr).to_bytes(4, byteorder='big')
    # if "srcPort" in flowkey.key:
    #     key_byte += (flowkey.src_port).to_bytes(2, byteorder='big')
    # if "dstPort" in flowkey.key:
    #     key_byte += (flowkey.dst_port).to_bytes(2, byteorder='big')
    # if "proto" in flowkey.key:
    #     key_byte += (flowkey.proto).to_bytes(1, byteorder='big')

    if "srcIP" in flowkey.key:
        key_byte += (flowkey.src_addr).to_bytes(4, byteorder='big')
    if "srcPort" in flowkey.key:
        key_byte += (flowkey.src_port).to_bytes(2, byteorder='big')
    if "dstIP" in flowkey.key:
        key_byte += (flowkey.dst_addr).to_bytes(4, byteorder='big')
    if "dstPort" in flowkey.key:
        key_byte += (flowkey.dst_port).to_bytes(2, byteorder='big')
    if "proto" in flowkey.key:
        key_byte += (flowkey.proto).to_bytes(1, byteorder='big')

    if poly not in crc_dict.keys():
        crc_dict[poly] = {}
    if init not in crc_dict[poly].keys():
        crc_dict[poly][init] = {}
    if xout not in crc_dict[poly][init].keys():
        crc_dict[poly][init][xout] = crcmod.mkCrcFun((1<<32)+poly, initCrc=init, rev=True, xorOut=xout)
    crc = crc_dict[poly][init][xout] (key_byte)
    # if isPrint:
    #     print("key(%d) poly(0x1%08x) init(%d) xout(%d) = %d %d" % (key, poly, init, xout, crc, crc%2048))
    return crc

def xxhash_hash(flowkey, params):
    if flowkey.key != "dstIP,dstPort":
        print("Don't know how to calculate xxhash_hash for flowkey:", flowkey.key)
        assert(False)

    key_bytes = b''
    key_bytes += (flowkey.dst_addr).to_bytes(4, byteorder='big')
    key_bytes += (flowkey.dst_port).to_bytes(4, byteorder='big')
    
    return xxhash.xxh32_intdigest(key_bytes[::-1], seed=xxhash_seeds.seeds_list[params['row']])

def compute_hash(flowkey, hash_function, params, max_value):
    return globals()[hash_function](flowkey, params) % max_value

    if hash == "universal_hash":
        return universal_hash(flowkey, params) % range

    if hash == "crc_hash":
        return crc_hash(flowkey, params) % range

    if hash == 'anup_hash':
        return anup_hash(flowkey, params) % range

    if hash == 'xxhash_hash':
        return xxhash_hash(flowkey, params) % range
