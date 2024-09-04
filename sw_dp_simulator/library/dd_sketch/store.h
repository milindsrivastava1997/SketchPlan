#ifndef DD_STORE_H
#define DD_STORE_H

#include <vector>

#include "utils.h"

using namespace std;

class Bins {
private:
    int _bins_length;
    int _bins_limit;
    //int _bins[MAX_SIZE];
    std::vector<int> _bins;

    void _check_index(int index);
public:
    Bins(int bins_limit);
    int get_length(void);
    int get_bin(int index);
    void set_bin(int index, int value);
    void add_to_bin(int index, int increment);
    //void grow_bins(int new_length);
    void extend_bins(int length_to_add);
    int get_sum(int start, int end);
    void set_zeroes(int start, int end);
    void truncate_right(int index);
    void truncate_left(int index);
    void clear();
};

class DD_Store {
protected:
    int _min_key;
    int _max_key;
    //Int _min_key;
    //Int _max_key;
    int _count;
    Bins _bins;

public:
    DD_Store();
    DD_Store(int bins_limit);
    void clear();
    virtual void add(int key, int weight) = 0;
    virtual ~DD_Store();
};

class DD_DenseStore : public DD_Store {
protected:
    int _offset;
    int _chunk_size;
    //int _bins_length;
    //int _bins[MAX_SIZE];
    //Bins _bins;

    int _get_index(int key);
    int _get_new_length(int new_min_key, int new_max_key);
    void _extend_range(int key);
    void _extend_range(int key, int second_key);
    void _adjust(int new_min_key, int new_max_key);
    void _shift_bins(int shift);
    void _center_bins(int new_min_key, int new_max_key);

public:
    DD_DenseStore(int bins_limit);

    void add(int key, int weight);
    int length(void);
};

class DD_CollapsingLowestDenseStore : public DD_DenseStore {
private:
    int _bins_limit;
    bool _is_collapsed;
    
    int _get_index(int key);
    int _get_new_length(int new_min_key, int new_max_key);
    void _adjust(int new_min_key, int new_max_key);
public:
    DD_CollapsingLowestDenseStore(int bins_limit);
};
#endif
