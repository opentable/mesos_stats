
class Metric:
    def __init__(self, path, name, *measurements):
        self.name = name
        self.path = path
        self.measurements = measurements
        self.data = []

    def Add(self, slave): self.data.append(slave[self.path])

    def Sum(self): return sum(self.data)

    def DatapointName(self, dp):
        return self.name.replace("[]", dp)

    def Datapoint(self, name, value):
        return (self.DatapointName(name), value)

    def Results(self):
        results = []
        for f in self.measurements:
            results += f(self)
        return results

def Sum(metric):
    result = sum(metric.data)    
    return [metric.Datapoint("sum", result)]

def Mean(metric):
    d = metric.data
    result = float(sum(d))/len(d) if len(d) > 0 else float('nan')
    return [metric.Datapoint("mean", result)]

def Each(scale=1):
    def Each_scale(metric):
        results = []
        for i, d in enumerate(metric.data):
            results.append(metric.Datapoint("{0}".format(i), metric.data[i]*scale))
        return results
    return Each_scale

