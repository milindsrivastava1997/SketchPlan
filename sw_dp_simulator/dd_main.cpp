#include "dd_iteration.h"

int main(int argc, char **argv) {
    DDSketch();
    LogCollapsingLowestDenseDDSketch(256, 0.1);
}
