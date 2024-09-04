#ifndef DD_UTILS_H
#define DD_UTILS_H

#include <limits>

class Int {
public:
    int value;

    Int() {}
    Int(int x) {value = x;}

    virtual bool operator <(int rhs) {
        return value < rhs;
    }
    virtual bool operator >(int rhs) {
        return value > rhs;
    }
    virtual bool operator <=(int rhs) {
        return value <= rhs;
    }
    virtual bool operator >=(int rhs) {
        return value >= rhs;
    }
    virtual bool operator <(Int rhs) {
        return value < rhs.value;
    }
    virtual bool operator >(Int rhs) {
        return value > rhs.value;
    }
    virtual bool operator <=(Int rhs) {
        return value <= rhs.value;
    }
    virtual bool operator >=(Int rhs) {
        return value >= rhs.value;
    }
    int operator -(int rhs) {
        return value - rhs;
    }
};

class _NegativeIntInfinity : public Int {
public:
    _NegativeIntInfinity() {}

    bool operator <(int rhs) {
        return true;
    }
    bool operator >(int rhs) {
        return false;
    }
};

class _PositiveIntInfinity : public Int {
public:
    _PositiveIntInfinity() {}

    bool operator <(int rhs) {
        return false;
    }
    bool operator >(int rhs) {
        return true;
    }
};

//_NegativeIntInfinity _neg_infinity = _NegativeIntInfinity();
//_PositiveIntInfinity _pos_infinity = _PositiveIntInfinity();
//Int _neg_infinity = _NegativeIntInfinity();
//Int _pos_infinity = _PositiveIntInfinity();
//int _neg_infinity = std::numeric_limits<int>::min();
//int _pos_infinity = std::numeric_limits<int>::max();

extern int _neg_infinity;
extern int _pos_infinity;

#endif
