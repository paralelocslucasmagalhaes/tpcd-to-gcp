import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from utils.beam_utils.split_csv_to_json import Split
from utils.beam_utils.join import ApplyMap, LeftJoin, JoinNested
import itertools
from datetime import datetime
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from utils.bqschema.schema import _FIELDS
import json
from beam_nuggets.io import relational_db



def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    table_schema = parse_table_schema_from_json(json.dumps(_FIELDS))

    p = beam.Pipeline(options=pipeline_options)

        #########################################################
    #
    #       GENERAL DATA
    ############################################################

    ## DATABASE CONFIGURATION
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
                        | 'Reading region data' >> relational_db.ReadFromDB(
                                source_config=source_config,
                                table_name='dss_region',
                                query='select r_regionkey, r_name from dss_region')
                        | 'Mapping region values' >> beam.Map(lambda element: (element['r_regionkey'], element['r_name']))
                )

    logging.info('Reading nation data..')
    pnation = (
                    p 
                        | 'Reading nation data' >> relational_db.ReadFromDB(
                                source_config=source_config,
                                table_name='dss_nation',
                                query="""select 
                                         n_nationkey as nationkey, 
                                         n_name as nation_name,
                                         n_regionkey as regionkey
                                         from dss_nation""")
                )

    ############################################################
    #
    #       CUSTOMER DATA
    ############################################################
    pcustomers = (
                    p 
                        | 'Reading customer data' >> relational_db.ReadFromDB(
                                source_config=source_config,
                                table_name='dss_customer',
                                query="""select 
                                         c_custkey as custkey, 
                                         c_name as customer_name,
                                         c_address as customer_addres,
                                         c_nationkey as nationkey,
                                         c_mktsegment as mktsegment
                                         from dss_customer """)
                )
    
    ## Enrich Customer Data
    logging.info('Enrich Customer Data..')
    pipeline_dict = {'customer':pcustomers, 'nation':pnation}
    pcustomer_nation = (
        pipeline_dict
            | 'Join Customer with Nations' >> LeftJoin('customer',pcustomers,'nation',pnation, 'nationkey')
            | 'Getting Region Name to Customer' >> ApplyMap('customer_region', 'regionkey', pregion)
            | 'Mapping customers values' >> beam.Map(lambda element: {
                                                                'custkey':element['custkey'], 
                                                                'customer_name' : element['customer_name'], 
                                                                'customer_addres': element['customer_addres'],
                                                                'mktsegment': element['mktsegment'],
                                                                'customer_nation' :  element['nation_name'],
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
                                                            from dss_order """)
                        | 'Reshuffling order data to be parallel' >> beam.Reshuffle()
                        | 'cleaning unncessary fields from order' >> beam.Map(lambda element:{
                                        'orderkey': element['orderkey'],
                                        'custkey': element['custkey'],
                                        'orderstatus': element['orderstatus'],
                                        'totalprice': element['totalprice'],
                                        'orderdate':element['orderdate'].strftime('%Y-%m-%d'),
                                        'orderpriority': element['orderpriority'],
                                        'shippriority': element['shippriority']
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
                        | 'Reading supplier data' >> relational_db.ReadFromDB(
                                                    source_config=source_config,
                                                    table_name='dss_supplier',
                                                    query="""select 
                                                            s_suppkey as suppkey,
                                                            s_name as supplier_name,
                                                            s_address as supplier_address,
                                                            s_nationkey as nationkey
                                                            from dss_supplier""")
                )

    ## Enrich Supplier Data
    logging.info('Enrich Supplier Data..')
    pipeline_dict = {'supplier':psupplier, 'supplier_nation':pnation}
    psupplier_nation = (
        pipeline_dict
            | 'Join Supplier with Nations' >> LeftJoin('supplier',psupplier, 'supplier_nation',pnation, 'nationkey')
            | 'Getting Region Name to Supplier' >> ApplyMap('supplier_region', 'regionkey', pregion)
            | 'Mapping supplier fields' >> beam.Map(lambda element:{
                        'suppkey': element['suppkey'],
                        'supplier_name': element['supplier_name'],
                        'supplier_address': element['supplier_address'],
                        'supplier_nation': element['nation_name'],
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
                        | 'Reading product data' >> relational_db.ReadFromDB(
                                                    source_config=source_config,
                                                    table_name='dss_part',
                                                    query="""select 
                                                            p_partkey as partkey,
                                                            p_name as product_name,
                                                            p_mfgr as product_manufacture,
                                                            p_brand as product_brand,
                                                            p_type  as product_type,
                                                            p_size   as product_size,
                                                            p_container as product_container,
                                                            p_retailprice as retailprice
                                                            from dss_part """)
                )


    #########################################################
    #
    #       PRODUCT AVAILABILITY BY SUPPLIER
    ############################################################


    logging.info('Reading Product Availability data..')
    ppsupp = (
                    p 
                        | 'Reading product Availability data' >> relational_db.ReadFromDB(
                                                                source_config=source_config,
                                                                table_name='dss_partsupp',
                                                                query="""select 
                                                                        CONCAT(ps_partkey, '|', ps_suppkey) as ckey,
                                                                        ps_availqty as availqty,
                                                                        ps_supplycost as supplycost
                                                                        from dss_partsupp """)
                )

    

    #########################################################
    #
    #       ITEMS DATA
    ############################################################

    logging.info('Reading items data..')
    pitems = (
                    p 
                        | 'Reading items data' >> relational_db.ReadFromDB(
                                                                source_config=source_config,
                                                                table_name='dss_lineitem',
                                                                query="""select 
                                                                        CONCAT(l_partkey, '|', l_suppkey) as ckey,
                                                                        l_orderkey as orderkey,
                                                                        l_partkey as partkey,
                                                                        l_suppkey as suppkey,
                                                                        l_linenumber,
                                                                        l_quantity,
                                                                        l_extendedprice,
                                                                        l_discount,
                                                                        l_tax,
                                                                        l_returnflag,
                                                                        l_linestatus,
                                                                        l_shipdate,
                                                                        l_commitdate,
                                                                        l_receiptdate,
                                                                        l_shipinstruct,
                                                                        l_shipmode
                                                                        from dss_lineitem""")
                        | 'Reshuffling items data to be parallel' >> beam.Reshuffle()
                        | 'Mapping items fields' >> beam.Map(lambda element: {
                                        'ckey':  element['ckey'],
                                        'orderkey': element['orderkey'],
                                        'partkey': element['partkey'],
                                        'suppkey': element['suppkey'],
                                        'linenumber': element['l_linenumber'],
                                        'quantity': element['l_quantity'],
                                        'extendedprice': element['l_extendedprice'],
                                        'discount': element['l_discount'],
                                        'tax': element['l_tax'],
                                        'returnflag': element['l_returnflag'],
                                        'linestatus': element['l_linestatus'],
                                        'shipdate': element['l_shipdate'].strftime('%Y-%m-%d'),
                                        'commitdate': element['l_commitdate'].strftime('%Y-%m-%d'),
                                        'receiptdate': element['l_receiptdate'].strftime('%Y-%m-%d'),
                                        'delay' : (element['l_commitdate'] - element['l_receiptdate']).days,
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