import apache_beam as beam
import itertools

class ApplyMap(beam.PTransform):

    """This PTransform performs a mapping given column name, common_key and join_data at constructors
       The column name will be a new field on pcollection
     
     Ex:
     'Join Customer Data to Order' >> ApplyMap('customer', 'customer_id', customers)

     Will be add a customer at pcolletion where is find on customers pcolletion
     
     """
    
    def __init__(self, column_name, column_join, join_data):
        super().__init__()
        self.column_name = ''.join(column_name)
        self.column_join = ''.join(column_join)
        self.join_data = join_data

    def apply_map(self, row, join_data):
        row[self.column_name] = join_data[row[self.column_join]]

        return row

    def expand(self, pcoll):
        return (pcoll | 'Mapping values for {0} to {1}'.format(self.column_join, self.column_name) >> beam.Map(self.apply_map, beam.pvalue.AsDict(self.join_data)))



class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors
     
     Ex:
     join | 'Left Join' >> LeftJoin(
                                        'orders', orders, 'customers', customer, 'customer_id')
     
     """

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        super().__init__()
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict
        
        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""

        return ({pipeline_name: (pcoll | 'Convert to ({0}, object) for {1}'.format(self.common_key, pipeline_name) >> beam.Map(_format_as_common_key_tuple, self.common_key)) for (pipeline_name, pcoll) in pcolls.items()}
                 | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                 | 'Unnest Cogrouped' >> beam.ParDo(_UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name,
                                                   self.common_key))

class _UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name, common_key):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]

        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                source_dictionary.pop(common_key)
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                source_dictionary.pop(common_key)
                yield source_dictionary



class JoinNested(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors
     
     Ex:
     join | 'Left Join' >> LeftJoin(
                                        'orders', orders, 'customers', customer, 'customer_id')
     
     """

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        super().__init__()
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict
        
        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""

        return ({pipeline_name: (pcoll | 'Convert to ({0}, object) for {1}'.format(self.common_key, pipeline_name) >> beam.Map(_format_as_common_key_tuple, self.common_key)) for (pipeline_name, pcoll) in pcolls.items()}
                 | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                 | 'Unnest Cogrouped' >> beam.ParDo(_CoGroupedNested(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name,
                                                   self.common_key
                                                   )
                 )


class _CoGroupedNested(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name, common_key):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        
        #cria lista para remover campo chave
        nested = []
        items = {}
        for line in join_dictionary:
            line.pop(common_key)
            nested.append(line)

        items[join_pipeline_name] = nested
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(items)
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                source_dictionary.pop(common_key)
                yield source_dictionary
