#include "traffic_shifts/injector.h"

uint32_t Injector::ip_to_int(const char * ip) {
    uint32_t ip_int = 0;
    for (int i = 0; i < 4; i++) {
        ip_int = ip_int << 8;
        ip_int += (uint32_t) ip[i];
    }
    return ip_int;
}

void Injector::init_inject_traffic(uint32_t number_of_flows, uint64_t flow_size, map<const char*, bool> flowkey_map) {
    // fixed flowkeys
    fixed_ip = ip_to_int("255.255.255.0");
    fixed_port = 0;

    // variable flowkeys
    uint32_t variable_start_ip = ip_to_int("10.0.0.0");
    
    for(uint32_t i = 1; i <= number_of_flows; i++) {
        variable_ips.push_back(variable_start_ip + i);
        variable_ports.push_back(i);
    }

    // set fixed fields of packet
    template_packet.timestamp = 0;
    template_packet.size = 0;
    template_packet.ip_proto = 4;

    last_flow_id = 0;
    last_flow_size = 0;

    this->number_of_flows = number_of_flows;
    this->flow_size = flow_size;
}

void Injector::next_inject_traffic(vector<packet_summary> &packets) {
    const uint32_t max_packets = 100000;

    uint32_t count = 0;

    uint32_t i = last_flow_id;
    uint64_t j = last_flow_size;
    bool break_loop = false;

    for (; i < variable_ips.size(); i++) {
        packet_summary packet = template_packet;
        // set fixed fields of packet
        if (flowkey_map["src_ip"]) {
            packet.src_ip = fixed_ip;
        } else {
            packet.src_ip = variable_ips[i];
        }
        if (flowkey_map["dst_ip"]) {
            packet.dst_ip = fixed_ip;
        } else {
            packet.dst_ip = variable_ips[i];
        }
        if (flowkey_map["src_port"]) {
            packet.src_port = fixed_port;
        } else {
            packet.src_port = variable_ports[i];
        }
        if (flowkey_map["dst_port"]) {
            packet.dst_port = fixed_port;
        } else {
            packet.dst_port = variable_ports[i];
        }

        for (; j < flow_size; j++) {
            packets.push_back(packet);
            count++;
            //cout << "Added packet " << count << " " << i << " " << j << endl;
            if (count == max_packets) {
                break_loop = true;
                break;
            }
        }

        if (break_loop) {
            break;
        }

        j = 0;
    }

    if (break_loop) {
        last_flow_size = j + 1;
        last_flow_id = i;
        if (last_flow_size == flow_size) {
            last_flow_size = 0;
            last_flow_id += 1;
        }
    }
    else {
        last_flow_size = j;
        last_flow_id = i;
    }
}

