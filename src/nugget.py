import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from utils.beam_utils.split_csv_to_json import Split
from utils.beam_utils.join import ApplyMap, LeftJoin, JoinNested
import itertools
from datetime import datetime
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import json
from beam_nuggets.io import relational_db

__SCHEMA = [
    {
      "description": "orderkey",
      "mode": "REQUIRED",
      "name": "orderkey",
      "type": "FLOAT"
    },
    {
      "description": "custkey",
      "mode": "REQUIRED",
      "name": "custkey",
      "type": "INTEGER"
    },
    {
      "description": "status da ordem",
      "mode": "NULLABLE",
      "name": "orderstatus",
      "type": "STRING"
    },

    
    {
      "description": "totalprice",
      "mode": "NULLABLE",
      "name": "totalprice",
      "type": "FLOAT"
    },

    {
        "description": "orderdate",
        "mode": "NULLABLE",
        "name": "orderdate",
        "type": "DATE"
    },
    {
        "description": "orderpriority",
        "mode": "NULLABLE",
        "name": "orderpriority",
        "type": "STRING"
    },
    {
        "description": "shippriority",
        "mode": "NULLABLE",
        "name": "shippriority",
        "type": "INTEGER"
    }]

_FIELDS = dict(fields = __SCHEMA)

def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input path')

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    # parser.add_argument('--host',
    #                     dest='host',
    #                     required=False,
    #                     help='Database host')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    table_schema = parse_table_schema_from_json(json.dumps(_FIELDS))

    # additional_bq_parameters = {
    #   'timePartitioning': {
    #                         'type': 'DAY',
    #                         'field': 'orderdate'}}
    
    # Scrip para conectar no banco de dados
    # db_config = DBConfig(drivername='postgresql', 
    #                                 username='postgres',
    #                                 password='Paralelo@2016$',
    #                                 database='perfectorder',
    #                                 host=known_args.host,
    #                                 port = 5432)

    ####### FILTERS EXAMPLES #######################
    #
    # today = date.today()
    # filters = "orders_order.due_date >= TO_DATE('{}', 'YYYY-MM-DD')".format(today)
    #
    # filters = "orders_order.customer_id = 3 AND orders_order.due_date >= TO_DATE('{}', 'YYYY-MM-DD')".format(today)
    #
    #####################################################################################################################

    p = beam.Pipeline(options=pipeline_options)

        #########################################################
    #
    #       GENERAL DATA
    ############################################################

    source_config = relational_db.SourceConfiguration(
        drivername='mysql+pymysql',
        host='relational.fit.cvut.cz',
        port=3306,
        username='guest',
        password='relational',
        database='tpcd',
    )

    logging.info('Reading region data..')
    pregion = (
                    p 
                        | 'Reading order data' >> relational_db.ReadFromDB(
                                                    source_config=source_config,
                                                    table_name='dss_order',
                                                    query="""select 
                                                            o_orderkey as orderkey,
                                                            o_custkey as custkey,
                                                            o_orderstatus as orderstatus,
                                                            o_totalprice as totalprice,
                                                            o_orderdate as orderdate,
                                                            o_orderpriority as orderpriority,
                                                            o_shippriority as shippriority
                                                            from dss_order limit 100""")
                        | 'cleaning unncessary fields from order' >> beam.Map(lambda element:{
                                        'orderkey': element['orderkey'],
                                        'custkey': element['custkey'],
                                        'orderstatus': element['orderstatus'],
                                        'totalprice': element['totalprice'],
                                        'orderdate': element['orderdate'].strftime('%Y-%m-%d'),
                                        'orderpriority': element['orderpriority'],
                                        'shippriority': element['shippriority']
                            }
                        )
                        | 'Writing to BQ' >> beam.io.WriteToBigQuery(
                                                        known_args.output,
                                                        'santodigital','perfect-order-api',
                                                        schema=table_schema,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

                )
                        

    
    p.run().wait_until_finish()



if __name__ == "__main__":

    #logging.basicConfig(filename='myapp.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    #logging.getLogger().setLevel(logging.INFO)

    main()