import statistics

def relative_error(true, estimate):
    if true == 0:
        return 0
    return abs(true-estimate)/true*100

def counter_diff(counter1, counter2):
    wrong = 0
    correct = 0
    index = 0
    sum = 0
    for a, b in zip(counter1, counter2):
        if a == b:
            correct += 1
            str = 'O'
        else:
            wrong += 1
            str = 'X'
        sum += abs(a-b)
        index += 1
    return wrong, sum

def get_counter_diff(previous_counter, tofino_counters):
    diff_counter = []
    for a, b in zip(previous_counter, tofino_counters):
        diff_counter.append(b-a)
    return diff_counter
