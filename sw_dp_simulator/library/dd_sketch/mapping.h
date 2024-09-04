#ifndef DD_MAPPING_H
#define DD_MAPPING_H

#include <limits>
#include <cmath>

using namespace std;

class DD_KeyMapping {
protected:
    double _relative_accuracy;

    double _offset;
    double _gamma;
    double _gamma_mantissa;
    double _multiplier;
    double _min_possible;
    double _max_possible;

    virtual float _log_gamma(float value) = 0;

public:
    DD_KeyMapping(float);
    float get_relative_accuracy();
    int get_key(float value);
};

class DD_LogarithmicMapping : public DD_KeyMapping {
protected:
    float _log_gamma(float value);
public:
    DD_LogarithmicMapping(float);
};

#endif
