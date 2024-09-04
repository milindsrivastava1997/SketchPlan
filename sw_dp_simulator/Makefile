CC=g++ -std=c++11
PROFILER= -pg
LINK = -lpcap

CFLAGS = -I$(sw_dp_simulator)

CPP_LIB_PATH = ${sketch_home}/cpp_lib
INC = -I${CPP_LIB_PATH}
cpp_lib = $(patsubst %.cpp, %.o, $(wildcard ${CPP_LIB_PATH}/*/*.cpp))

file_io_gt = $(patsubst %.cpp, %.o, $(wildcard file_io/cpp/ground_truth/*.cpp))
file_io_sw_simulator = $(patsubst %.cpp, %.o, $(wildcard file_io/cpp/sw_simulator/*.cpp))
file_io_common = $(patsubst %.cpp, %.o, $(wildcard file_io/cpp/common/*.cpp))
hash_module = $(patsubst %.cpp, %.o, $(wildcard hash_module/cpp/*.cpp))
library_objects = $(patsubst %.cpp, %.o, $(wildcard library/*.cpp))
dd_library_objects = $(patsubst %.cpp, %.o, $(wildcard library/dd_sketch/*.cpp))
sketches = $(patsubst %.cpp, %.o, $(wildcard sketches/*.cpp))
traffic_shifts = $(patsubst %.cpp, %.o, $(wildcard traffic_shifts/*.cpp))

all: $(file_io_gt) $(file_io_sw_simulator) $(file_io_common) $(hash_module) $(library_objects) $(dd_library_objects) $(sketches) $(cpp_lib) main main_streaming $(traffic_shifts)

%.o: %.cpp %.h
	$(CC) $(CFLAGS) $(INC) -c -Wall $< -o $@
#	@echo $@ # .o
#	@echo $< # .cpp
#	@echo $^ # .cpp .h

main: main.cpp $(file_io_gt) $(file_io_sw_simulator) $(file_io_common) $(hash_module) $(library_objects) $(dd_library_objects) $(sketches) $(cpp_lib) $(traffic_shifts)
	$(CC) $(CFLAGS) -o main main.cpp $(file_io_gt) $(file_io_sw_simulator) $(file_io_common) $(hash_module) $(library_objects) $(dd_library_objects) $(sketches) $(traffic_shifts) $(cpp_lib) $(LINK) $(INC)

main_streaming: main_streaming.cpp $(file_io_gt) $(file_io_sw_simulator) $(file_io_common) $(hash_module) $(library_objects) $(dd_library_objects) $(sketches) $(cpp_lib) $(traffic_shifts)
	$(CC) $(CFLAGS) -o main_streaming main_streaming.cpp $(file_io_gt) $(file_io_sw_simulator) $(file_io_common) $(hash_module) $(library_objects) $(dd_library_objects) $(sketches) $(cpp_lib) $(traffic_shifts) $(LINK) $(INC)

clean:
	rm -f main main_streaming
	rm -f file_io/ground_truth/cpp/*.o
	rm -f file_io/sw_simulator/cpp/*.o
	rm -f file_io/common/cpp/*.o
	rm -f hash_module/cpp/*.o
	rm -f library/*.o
	rm -f library/dd_sketch/*.o
	rm -f sketches/*.o
	rm -f traffic_shifts/*.o
	rm -f ${sketch_home}/cpp_lib/*/*.o

