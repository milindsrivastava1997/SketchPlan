#include "dd_iteration.h"

float DEFAULT_RELATIVE_ACCURACY = 0.01;
int DEFAULT_BIN_LIMIT = 1024;

//ddIteration::ddIteration()
//{
//}
//
//void ddIteration::init(parameters &params)
//{
//    sampling_hash = new HashSeedSet(25);
//    for(int level=0; level<25; level++) {
//        dd_level.push_back(sketchTemplate(params, level));
//    }
//
//    // TODO: implement positive and negative infinity
//    // from ddsketch/store.py
//    _min_key = 
//
//}
//
//void ddIteration::load_hash(parameters &params) {
//    load_hash_seeds(*sampling_hash, dd_level, params);
//    for(int i = 0; i < 25; i++) {
//        if (params.is_same_level_hash == 1) {
//            dd_level[i].index_hash = dd_level[0].index_hash;
//            dd_level[i].res_hash = dd_level[0].res_hash;
//        }
//    }
//}
//
//void ddIteration::iteration(packet_summary &p, parameters &params)
//{
//    // TODO:
//    int elem = params.is_count_packet ? 1 : p.size;
//    flowkey_t flowkey;
//    get_flowkey(flowkey, p, params);
//    dd_level[0].dd_sketch(flowkey, params, elem);
//
//    if (packetMap.find(flowkey) == packetMap.end()) {
//        packetMap[flowkey] = elem;
//    }
//    else {
//        packetMap[flowkey] = packetMap[flowkey] + elem;
//    }
//}
//
//void ddIteration::file_print(parameters &params)
//{
//    dd_file_print(dd_level, *sampling_hash, params, packetMap, 1000);
//}
//
//void ddIteration::clear(parameters &params)
//{
//    dd_level[0].clear();
//    packetMap.clear();
//}

BaseDDSketch::BaseDDSketch(DD_LogarithmicMapping mapping, DD_DenseStore store, int zero_count) : _store(store), _mapping(mapping) {
    //_mapping = mapping;
    //_store = store;
    _zero_count = zero_count;
    _relative_accuracy = mapping.get_relative_accuracy();
}

void BaseDDSketch::init(parameters &params) {
    sampling_hash = new HashSeedSet(25);
    for(int level=0; level<25; level++) {
        dd_level.push_back(sketchTemplate(params, level));
    }
}

void BaseDDSketch::load_hash(parameters &params) {
    load_hash_seeds(*sampling_hash, dd_level, params);
    for(int i = 0; i < 25; i++) {
        if (params.is_same_level_hash == 1) {
            dd_level[i].index_hash = dd_level[0].index_hash;
            dd_level[i].res_hash = dd_level[0].res_hash;
        }
    }
}

void BaseDDSketch::iteration(packet_summary &p, parameters &params) {
    int elem = params.is_count_packet ? 1 : p.size;
    flowkey_t flowkey;
    get_flowkey(flowkey, p, params);
    int flowkey_hash = dd_level[0].index_hash->compute_hash(flowkey, params.flowkey_flags, 0, params.is_crc_hash, std::numeric_limits<int>::max());
    _store.add(_mapping.get_key(flowkey_hash), elem);
}

void BaseDDSketch::file_print(parameters &params) {
    ;
}

void BaseDDSketch::clear(parameters &params) {
    _store.clear();
}

BaseDDSketch::~BaseDDSketch() {
    ;
}

DDSketch::DDSketch() : BaseDDSketch(DD_LogarithmicMapping(DEFAULT_RELATIVE_ACCURACY), DD_DenseStore(DEFAULT_BIN_LIMIT), 0) {
    _relative_accuracy = DEFAULT_RELATIVE_ACCURACY;
}

DDSketch::DDSketch(float relative_accuracy) : BaseDDSketch(DD_LogarithmicMapping(relative_accuracy), DD_DenseStore(DEFAULT_BIN_LIMIT), 0) {
    _relative_accuracy = relative_accuracy;
}

DDSketch::~DDSketch() {
    ;
}

LogCollapsingLowestDenseDDSketch::LogCollapsingLowestDenseDDSketch(int bins_limit) : BaseDDSketch(DD_LogarithmicMapping(DEFAULT_RELATIVE_ACCURACY), DD_DenseStore(_bins_limit), 0)  {
    _bins_limit = bins_limit;
    _relative_accuracy = DEFAULT_RELATIVE_ACCURACY;
}

LogCollapsingLowestDenseDDSketch::LogCollapsingLowestDenseDDSketch(int bins_limit, float relative_accuracy) : BaseDDSketch(DD_LogarithmicMapping(relative_accuracy), DD_DenseStore(_bins_limit), 0)  {
    _bins_limit = bins_limit;
    _relative_accuracy = relative_accuracy;
}

LogCollapsingLowestDenseDDSketch::~LogCollapsingLowestDenseDDSketch() {
    ;
}
