import apache_beam as beam


class Split(beam.DoFn):

    def __init__(self, columns):
        self.columns = columns

    def process(self, element):
        #result = [{(self.columns[], values) for values in element.split(';')}]
        # tupla_values = dict(zip(self.columns, element.split(';')))
        
        return [dict(zip(self.columns, element.split(';')))]