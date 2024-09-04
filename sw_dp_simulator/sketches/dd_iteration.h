#ifndef DD_H
#define DD_H

#include <vector>
#include <iostream>
#include <map>
#include <limits>
#include <cmath>

#include <stdio.h>
#include <math.h>

#include "sketch_iteration_template.h"

#include "library/params.h"
#include "library/flowkey.h"
#include "library/pcap_helper.h"
#include "library/timer.h"
#include "library/priority_queue.h"
#include "library/sketch_template.h"

#include "library/dd_sketch/mapping.h"
#include "library/dd_sketch/store.h"

#include "hash_module/cpp/crc.h"
#include "hash_module/cpp/hash.h"
#include "hash_module/cpp/seed.h"
#include "file_io/cpp/sw_simulator/file_print.h"

using namespace std;

//class ddIteration : public sketchIterationTemplate {
//private:
//
//public:
//    double relative_accuracy;
//
//    map <flowkey_t, int> packetMap;
//
//    HashSeedSet *sampling_hash;
//    //vector<sketchTemplate> dd_level;
//    ddIteration();
//
//    void init(parameters &params);
//    void load_hash(parameters &params);
//    void iteration(packet_summary &p, parameters &params);
//    void file_print(parameters &params);
//    void clear(parameters &params);
//
//};

class BaseDDSketch : public sketchIterationTemplate {
protected:
    DD_DenseStore _store;
    DD_LogarithmicMapping _mapping;
    int _zero_count;
    float _relative_accuracy;

    HashSeedSet *sampling_hash;
    vector<sketchTemplate> dd_level;

public:
    BaseDDSketch();
    BaseDDSketch(DD_LogarithmicMapping, DD_DenseStore, int);

    void init(parameters &params);
    void load_hash(parameters &params);
    // equivalent to `add` in ddsketch/ddsketch.py
    void iteration(packet_summary &p, parameters &params);
    void file_print(parameters &params);
    void clear(parameters &params);
    ~BaseDDSketch();
};

class DDSketch : public BaseDDSketch {
public:
    DDSketch();
    DDSketch(float relative_accuracy);
    // do initialization here
    //void init(parameters &params);
    ~DDSketch();
};

class LogCollapsingLowestDenseDDSketch : public BaseDDSketch {
private:
    int _bins_limit;

public:
    LogCollapsingLowestDenseDDSketch(int bins_limit);
    LogCollapsingLowestDenseDDSketch(int bins_limit, float relative_accuracy);
    // do initialization here
    //void init(parameters &params);
    ~LogCollapsingLowestDenseDDSketch();
};

#endif // DD_H
