#include "mapping.h"

// logic from ddsketch/mapping.py

DD_KeyMapping::DD_KeyMapping(float relative_accuracy) {
    _relative_accuracy = relative_accuracy;
    
    _offset = 0.0;

    _gamma_mantissa = 2 * relative_accuracy / (1 - relative_accuracy);
    _gamma = 1 + _gamma_mantissa;
    _multiplier = 1 / log1p(_gamma_mantissa);
    _multiplier *= log(2);
    _min_possible = numeric_limits<float>::min() * _gamma;
}

float DD_KeyMapping::get_relative_accuracy() {
    return _relative_accuracy;
}

int DD_KeyMapping::get_key(float value) {
    return int(ceil(_log_gamma(value)) + _offset);
}

DD_LogarithmicMapping::DD_LogarithmicMapping(float relative_accuracy) : DD_KeyMapping(relative_accuracy) {
    _multiplier *= log(2);
}

float DD_LogarithmicMapping::_log_gamma(float value) {
    return log2(value) * _multiplier;
}
