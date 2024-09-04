#include <cmath>
#include <cassert>
#include <algorithm>

#include "store.h"

int CHUNK_SIZE = 128;
int MAX_BINS_SIZE = 10000;

Bins::Bins(int bins_limit) {
    assert(bins_limit <= MAX_BINS_SIZE);
    _bins.reserve(bins_limit);
    _bins_limit = bins_limit;
    _bins_length = 0;
    set_zeroes(0, _bins_limit);
    //for(int i = 0; i < _bins_limit; i++) {
    //    _bins[i] = 0;
    //}
}

void Bins::_check_index(int index) {
    assert(index > 0 && index < _bins_length);
}

int Bins::get_length() {
    return _bins_length;
}

int Bins::get_bin(int index) {
    _check_index(index);
    return _bins[index];
}

void Bins::set_bin(int index, int value) {
    _check_index(index);
    _bins[index] = value;
}

void Bins::add_to_bin(int index, int increment) {
    _check_index(index);
    _bins[index] += increment;
}

//void Bins::grow_bins(int new_length) {
//    if (new_length > _bins_limit) {
//        assert(false);
//    }
//    _bins_length = new_length;
//}

void Bins::extend_bins(int length_to_add) {
    assert(_bins_length + length_to_add <= _bins_limit);
    _bins_length += length_to_add;
    set_zeroes(_bins_length - length_to_add, _bins_length);
}

// NOTE: sum excludes element at `end`
int Bins::get_sum(int start, int end) {
    _check_index(start);
    //assert(start > 0);
    //assert(start < _bins_length);
    assert(end > 0);
    assert(end <= _bins_length);
    
    int result = 0;
    for(int i = start; i < end; i++) {
        result += _bins[i];
    }
    return result;
}

// NOTE: setting zeroes excludes element at `end`
void Bins::set_zeroes(int start, int end) {
    _check_index(start);
    assert(end > 0);
    assert(end <= _bins_length);

    fill(_bins.begin() + start, _bins.begin() + end, 0);
    //for(int i = start; i < end; i++) {
    //    _bins[i] = 0;
    //}
}

void Bins::truncate_right(int index) {
    if (index < 0) {
        index += _bins_length;
    }
    _bins_length = index;
}

void Bins::truncate_left(int index) {
    if (index < 0) {
        index += _bins_length;
    }
    // copy element from [index:] to [0:]
    // TODO: verify
    for(auto it = _bins.begin() + index; it != _bins.end(); it++) {
        *(it - index) = *it;
    }
    //std::shift_left(_bins.begin() + index, _bins.end(), index);
    // reduce length
    _bins_length -= index;
}

void Bins::clear() {
    set_zeroes(0, get_length());
}

DD_Store::DD_Store() : _bins(MAX_BINS_SIZE) {
    _min_key = _pos_infinity;
    _max_key = _neg_infinity;
    _count = 0;
}

DD_Store::DD_Store(int bins_limit) : _bins(bins_limit) {
    _min_key = _pos_infinity;
    _max_key = _neg_infinity;
    _count = 0;
}

void DD_Store::clear() {
    _bins.clear();
}

DD_Store::~DD_Store() {
    ;
}

DD_DenseStore::DD_DenseStore(int bins_limit) : DD_Store(bins_limit) {
    _offset = 0;
    _chunk_size = CHUNK_SIZE;
    //_bins_length = 0;
}

int DD_DenseStore::_get_index(int key) {
    //if (key < _min_key) {
    if (_min_key > key) {
        _extend_range(key);
    }
    //else if (key > _max_key) {
    else if (_max_key < key) {
        _extend_range(key);
    }
    return key - _offset;
}

int DD_DenseStore::_get_new_length(int new_min_key, int new_max_key) {
    int desired_length = new_max_key - new_min_key + 1;
    return _chunk_size * int(ceil(desired_length / _chunk_size));
}

void DD_DenseStore::_extend_range(int key) {
    _extend_range(key, key);
}

void DD_DenseStore::_extend_range(int key, int second_key) {
    int new_min_key = min(min(key, second_key), _min_key);
    int new_max_key = max(max(key, second_key), _max_key);

    if (length() == 0) {
        // below line replaced
        //_bins = [0.0] * _get_new_length(new_min_key, new_max_key);
        _bins.extend_bins(_get_new_length(new_min_key, new_max_key) - length());
        _offset = new_min_key;
        _adjust(new_min_key, new_max_key);
    }
    //else if ((new_min_key >= _min_key) && (new_max_key < _offset + length())) {
    else if ((_min_key <= new_min_key) && (new_max_key < _offset + length())) {
        _min_key = new_min_key;
        _max_key = new_max_key;
    }
    else {
        int new_length = _get_new_length(new_min_key, new_max_key);
        if (new_length > length()) {
            // below line replaced
            //_bins.extend([0.0] * (new_length - length()));
            _bins.extend_bins(new_length - length());
        }
        _adjust(new_min_key, new_max_key);
    }
}

void DD_DenseStore::_adjust(int new_min_key, int new_max_key) {
    _center_bins(new_min_key, new_max_key);
    _min_key = new_min_key;
    _max_key = new_max_key;
}

void DD_DenseStore::_shift_bins(int shift) {
    if (shift > 0) {
        // below line replaced
        //_bins = _bins[:-shift];
        _bins.truncate_right(-shift);
        //_bins[:0] = [0.0] * shift;
        // TODO: verify that above line does nothing
    }
    else {
        // below line replaced
        //_bins = _bins[abs(shift) :];
        _bins.truncate_left(abs(shift));
        //_bins.extend([0.0] * abs(shift));
        // below line replaced
        _bins.extend_bins(abs(shift));
    }
    _offset -= shift;
}

void DD_DenseStore::_center_bins(int new_min_key, int new_max_key) {
    int middle_key = new_min_key + (new_max_key - new_min_key + 1) / 2;
    _shift_bins(_offset + length() / 2 - middle_key);
}

void DD_DenseStore::add(int key, int weight) {
    int idx = _get_index(key);
    // below line replaced
    //_bins[idx] += weight;
    _bins.add_to_bin(idx, weight);
    _count += weight;
}

int DD_DenseStore::length(void) {
    //return _bins_length;
    return _bins.get_length();
}

DD_CollapsingLowestDenseStore::DD_CollapsingLowestDenseStore(int bins_limit) : DD_DenseStore(bins_limit) {
    _bins_limit = bins_limit;
    _is_collapsed = false;
}

int DD_CollapsingLowestDenseStore::_get_new_length(int new_min_key, int new_max_key) {
    int desired_length = new_max_key - new_min_key + 1;
    return min(_chunk_size * int(ceil(desired_length / _chunk_size)), _bins_limit);
}

int DD_CollapsingLowestDenseStore::_get_index(int key) {
    //if (key < _min_key) {
    if (_min_key > key) {
        if (_is_collapsed) {
            return 0;
        }

        _extend_range(key);
        if (_is_collapsed) {
            return 0;
        }
    }
    //else if (key > _max_key) {
    else if (_max_key < key) {
        _extend_range(key);
    }

    return key - _offset;
}

void DD_CollapsingLowestDenseStore::_adjust(int new_min_key, int new_max_key) {
    if (new_max_key - new_min_key + 1 > length()) {
        new_min_key = new_max_key - length() + 1;

        //if (new_min_key >= _max_key) {
        if (_max_key <= new_min_key) {
            _offset = new_min_key;
            _min_key = new_min_key;
            // below line replaced
            //_bins[:] = [0.0] * length();
            _bins.clear();
            // below line replaced
            //_bins[0] = _count;
            _bins.set_bin(0, _count);
        }
        else {
            int shift = _offset - new_min_key;
            if (shift < 0) {
                int collapse_start_index = _min_key - _offset;
                int collapse_end_index = new_min_key - _offset;
                // below line replaced
                //int collapsed_count = sum(
                //    _bins[collapse_start_index:collapse_end_index]
                //)
                int collapsed_count = _bins.get_sum(collapse_start_index, collapse_end_index);
                // below line replaced
                //_bins[collapse_start_index:collapse_end_index] = [0.0] * (
                //    new_min_key - _min_key
                //)
                _bins.set_zeroes(collapse_start_index, collapse_end_index);
                // below line replaced
                //_bins[collapse_end_index] += collapsed_count;
                _bins.add_to_bin(collapse_end_index, collapsed_count);
                _min_key = new_min_key;
                _shift_bins(shift);
            }
            else {
                _min_key = new_min_key;
                _shift_bins(shift);
            }
        }

        _max_key = new_max_key;
        _is_collapsed = true;
    }
    else {
        _center_bins(new_min_key, new_max_key);
        _min_key = new_min_key;
        _max_key = new_max_key;
    }
}
