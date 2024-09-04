#pragma once

#include "pcap/parser.h"
#include <vector>
#include <map>

vector<packet_summary> inject_traffic(uint32_t number_of_flows, uint64_t flow_size, map<const char*, bool> flowkey_map);

class Injector {
private:
    uint32_t fixed_ip;
    uint16_t fixed_port;
    vector<uint32_t> variable_ips;
    vector<uint16_t> variable_ports;
    packet_summary template_packet;
    map<const char*, bool> flowkey_map;

    uint32_t number_of_flows;
    uint64_t flow_size;

    uint32_t last_flow_id;
    uint64_t last_flow_size;

    uint32_t ip_to_int(const char * ip);

public:
    void init_inject_traffic(uint32_t number_of_flows, uint64_t flow_size, map<const char*, bool> flowkey_map);
    void next_inject_traffic(vector<packet_summary> &packets);
};
