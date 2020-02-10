import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from utils.beam_utils.split_csv_to_json import Split
from utils.beam_utils.join import ApplyMap, LeftJoin, JoinNested
from datetime import datetime
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from utils.bqschema.schema import _FIELDS
import json


def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(argv=None):
    parser = argparse.ArgumentParser()

    # parser.add_argument('--input',
    #                     dest='input',
    #                     required=True,
    #                     help='Input path')  
    

    # parser.add_argument('--output',
    #                     dest='output',
    #                     required=True,
    #                     help='Output file to write results to.')

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

    # _region = f"{known_args.input}dss_region.csv"
    logging.info('Reading region data..')
    pregion = (
                    p 
                        | 'Reading region data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='region', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'Mapping region values' >> beam.Map(lambda element: (element['r_regionkey'], element['r_name'] ))
                )

    logging.info('Reading nation data..')
    pnation = (
                    p 
                        | 'Reading nation data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='nation', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                )

        #########################################################
    #
    #       CUSTOMER DATA
    ############################################################

    logging.info('Reading customer data..')
    pcustomers = (
                    p 
                        | 'Reading customer data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='customer', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))

                        | 'Mapping customer columns values' >> beam.Map(lambda element: {
                                                    'custkey' : element['c_custkey'],
                                                    'customer_name' : element['c_name'], 
                                                    'mktsegment': element['c_mktsegment'],
                                                    'n_nationkey' : element['c_nationkey']
                                                    })

                )
    
    ## Enrich Customer Data
    logging.info('Enrich Customer Data..')
    pipeline_dict = {'customer':pcustomers, 'nation':pnation}
    pcustomer_nation = (
        pipeline_dict
            | 'Join Customer with Nations' >> LeftJoin('customer',pcustomers,'nation',pnation, 'n_nationkey')
            | 'Getting Region Name to Customer' >> ApplyMap('customer_region', 'n_regionkey', pregion)
            | 'Mapping customers values' >> beam.Map(lambda element: {
                                                                'custkey':element['custkey'], 
                                                                'customer_name' : element['customer_name'], 
                                                                'mktsegment': element['mktsegment'],
                                                                'customer_nation' :  element['n_name'],
                                                                'customer_region' : element['customer_region']
                                                                } )
    )

        #########################################################
    #
    #       ORDER DATA
    ############################################################
    
    # Getting order data
    logging.info('Reading order data..')
    porder = (
                    p 
                        | 'Reading order data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='order', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'cleaning unncessary fields from order' >> beam.Map(lambda element:{
                                        'orderkey': element['o_orderkey'],
                                        'custkey': element['o_custkey'],
                                        'orderstatus': element['o_orderstatus'],
                                        'totalprice': element['o_totalprice'],
                                        'orderdate': element['o_orderdate'],
                                        'orderpriority': element['o_orderpriority'],
                                        'shippriority': element['o_shippriority']
                            }
                        )
                                        
                )

    logging.info('Join order data with customer data')
    pipeline_dict = {'orders':porder, 'customers':pcustomer_nation}
    porder_customer = (
        pipeline_dict
            | 'Join Order with Customer' >> LeftJoin('orders', porder, 'customers', pcustomer_nation, 'custkey')

    )

    #########################################################
    #
    #       ITEMS DATA
    # NESTED FIELDS
    ############################################################


    #########################################################
    #
    #       SUPPLIER DATA
    ############################################################

    logging.info('Reading Supplier data..')
    psupplier = (
                    p 
                        | 'Reading supplier data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='supplier', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'Cleaning supplier data' >> beam.Map(lambda e: {
                                                                    'suppkey' : e['s_suppkey'],
                                                                    'supplier_name' : e['s_name'],
                                                                    'n_nationkey': e['s_nationkey']


                        })
                )

    ## Enrich Supplier Data
    logging.info('Enrich Supplier Data..')
    pipeline_dict = {'supplier':psupplier, 'supplier_nation':pnation}
    psupplier_nation = (
        pipeline_dict
            | 'Join Supplier with Nations' >> LeftJoin('supplier',psupplier, 'supplier_nation',pnation, 'n_nationkey')
            | 'Getting Region Name to Supplier' >> ApplyMap('supplier_region', 'n_regionkey', pregion)
            | 'Mapping supplier fields' >> beam.Map(lambda element:{
                        'suppkey': element['suppkey'],
                        'supplier_name': element['supplier_name'],
                        'supplier_nation': element['n_name'],
                        'supplier_region' : element['supplier_region']

            })

    )


    #########################################################
    #
    #       PRODUCT DATA
    ############################################################

    logging.info('Reading Product data..')
    pproduct = (
                    p 
                        | 'Reading product data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='dss_part', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'Product mapping values' >> beam.Map(lambda element: {
                            'partkey': element['p_partkey'],
                            'product_name': element['p_name'],
                            'product_manufacture': element['p_mfgr'],
                            'product_brand': element['p_brand'],
                            'product_type': element['p_type'],
                            'product_size': element['p_size'],
                            'product_container': element['p_container'],
                            'retailprice': element['p_retailprice']
                        })
                )


    #########################################################
    #
    #       PRODUCT AVAILABILITY BY SUPPLIER
    ############################################################


    logging.info('Reading Product Availability data..')
    ppsupp = (
                    p 
                        | 'Reading product Availability data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='dss_partsupp', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'Creating Complex Key for Product and Supplier' >> beam.Map(lambda element:{
                                                                'ckey': "{}|{}".format(element['ps_partkey'], element['ps_suppkey']),
                                                                'availqty': element['ps_availqty'],
                                                                'supplycost': element['ps_supplycost']
            })
                )

    

    #########################################################
    #
    #       ITEMS DATA
    ############################################################

    logging.info('Reading items data..')
    pitems = (
                    p 
                        | 'Reading items data' >> beam.io.Read(beam.io.BigQuerySource(
                                                                            table='lineitem', 
                                                                            dataset='tpcd', 
                                                                            project='cs-data-warehouse-dev'))
                        | 'Mapping items fields' >> beam.Map(lambda element: {
                                        'ckey': "{}|{}".format(element['l_partkey'], element['l_suppkey']),
                                        'orderkey': element['l_orderkey'],
                                        'partkey': element['l_partkey'],
                                        'suppkey': element['l_suppkey'],
                                        'linenumber': element['l_linenumber'],
                                        'quantity': element['l_quantity'],
                                        'extendedprice': element['l_extendedprice'],
                                        'discount': element['l_discount'],
                                        'tax': element['l_tax'],
                                        'returnflag': element['l_returnflag'],
                                        'linestatus': element['l_linestatus'],
                                        'shipdate': element['l_shipdate'],
                                        'commitdate': element['l_commitdate'],
                                        'receiptdate': element['l_receiptdate'],
                                        'shipinstruct': element['l_shipinstruct'],
                                        'shipmode': element['l_shipmode']
                                        }

                        )
                )
    

    ## Enrich Items Data
    logging.info('Enrich Items Data..')
    pipeline_dict = {'item':pitems, 'product':pproduct}
    pitems_product = (
        pipeline_dict
            | 'Join Items with Product' >> LeftJoin('item', pitems, 'product',pproduct, 'partkey')
    )

    ## Enrich Items Data
    logging.info('Enrich Items Data..')
    pipeline_dict = {'item':pitems_product, 'supplier': psupplier_nation}
    pitems_supp = (
        pipeline_dict
            | 'Join items with Supplier' >> LeftJoin('item',pitems_product, 'supplier', psupplier_nation, 'suppkey')
    )


    # Enrich Items Data
    logging.info('Enrich Items Data..')
    pipeline_dict = {'item':pitems_supp, 'avail': ppsupp}
    pitems_availability = (
        pipeline_dict
            | 'Join items with avail' >> LeftJoin('item', pitems_supp, 'avail', ppsupp, 'ckey')
    )



     #########################################################
    #
    #       ADD ITEMS TO ORDERS AND WRITE
    ############################################################

    pipeline_dict = {'orders':porder_customer, 'items':pitems_availability}
    results = (
        pipeline_dict
            | 'Join Order with Items' >> JoinNested('orders',porder_customer,'items',pitems_availability, 'orderkey')
            # | 'Writing to BQ' >> beam.io.WriteToText(known_args.output)
            | 'Writing to BQ' >> beam.io.WriteToBigQuery(
                                                        table = 'orders',
                                                        dataset='tpcd',
                                                        project='cs-data-warehouse-dev',
                                                        schema=table_schema,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


    )
                
    p.run().wait_until_finish()



if __name__ == "__main__":

    #logging.basicConfig(filename='myapp.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    #logging.getLogger().setLevel(logging.INFO)

    main()