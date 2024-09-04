class Metric:
    def __init__(self, name, true_value, estimated_value, error):
        self.name = name
        self.true_value = true_value
        self.estimated_value = estimated_value
        self.error = error

    def __str__(self):
        return '{}, {}, {}, {}'.format(self.name, self.true_value, self.estimated_value, self.error)

    def __repr__(self):
        return str(self)
