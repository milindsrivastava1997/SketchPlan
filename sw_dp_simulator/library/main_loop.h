#ifndef __MAIN_LOOP_H
#define __MAIN_LOOP_H

#include <iostream>
#include <vector>

#include <stdio.h>
#include <string.h>

#include "library/timer.h"
#include "library/pcap_helper.h"
#include "sketches/sketch_iteration_template.h"
#include "library/params.h"
#include "traffic_shifts/injector.h"

void main_loop_streaming(parameters &params, sketchIterationTemplate* sketch_iteration_instance, pcap_t* handle);

void main_loop(parameters &params, vector<packet_summary> &packet_stream, sketchIterationTemplate* sketch_iteration_instance);

void main_loop_pcount(parameters &params, vector<packet_summary> &packet_stream, sketchIterationTemplate* sketch_iteration_instance);

void main_loop_inject(parameters &params, sketchIterationTemplate* sketch_iteration_instance, int& total_count, int& pcount);
void main_loop_sketchmd(parameters &params, vector<packet_summary> &packet_stream, sketchIterationTemplate* sketch_iteration_instance);

#endif
