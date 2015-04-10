
class Metric:
    def __init__(self, path, name, *measurements):
        self.name = name
        self.path = path
        self.measurements = measurements
        self.data = []

    def Add(self, datum, keys=[]): 
        self.data.append((datum[self.path], keys))

    def DatapointName(self, keys):
        clean_keys = ()
        for k in keys:
            clean = k.replace(".", "_")
            clean_keys += (clean,)
        return self.name.replace("[]", "%s") % clean_keys

    def Datapoint(self, keys, value):
        return (self.DatapointName(keys), value)

    def Results(self):
        results = []
        for f in self.measurements:
            results += f(self)
        return results

def Each(scale=1):
    def Each_scale(metric):
        results = []
        for i, dk in enumerate(metric.data):
            d, keys = dk 
            results.append(metric.Datapoint(keys, metric.data[i]*scale))
        return results
    return Each_scale

def Sum(metric):
    result = sum(metric.data)    
    return [metric.Datapoint("sum", result)]

def Mean(metric):
    d = metric.data
    result = float(sum(d))/len(d) if len(d) > 0 else float('nan')
    return [metric.Datapoint("mean", result)]

