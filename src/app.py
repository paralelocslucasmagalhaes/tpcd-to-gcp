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
                                         from dss_customer""")
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
                                                            from dss_order""")
                        | 'Reshuffling order data to be parallel' >> beam.Reshuffle()
                        | 'cleaning unncessary fields from order' >> beam.Map(lambda element:{
                                        'orderkey': element['orderkey'],
                                        'custkey': element['custkey'],
                                        'orderstatus': element['orderstatus'],
                                        'totalprice': element['totalprice'],
                                        'orderdate': datetime.strptime(element['orderdate'], '"%Y-%m-%d"').strftime('%Y-%m-%d'),
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
    _supplier_colums = ['suppkey','supplier_name','supplier_address','nationkey','phone','acctbal','s_comment']
    _supplier = f"{known_args.input}dss_supplier.csv"
    psupplier = (
                    p 
                        | 'Reading supplier data' >> beam.io.ReadFromText(_supplier, skip_header_lines=1)
                        | 'Mapping supplier data to Json' >> beam.ParDo(Split(columns=_supplier_colums))
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
    _product_colums = ['partkey','product_name','product_manufacture','product_brand','product_type','product_size','product_container','retailprice','product_comment']
    _product = f"{known_args.input}dss_part.csv"
    pproduct = (
                    p 
                        | 'Reading product data' >> beam.io.ReadFromText(_product, skip_header_lines=1)
                        | 'Mapping product data to Json' >> beam.ParDo(Split(columns=_product_colums))
                        | 'Product mapping values' >> beam.Map(lambda element: {
                            'partkey': element['partkey'],
                            'product_name': element['product_name'],
                            'product_manufacture': element['product_manufacture'],
                            'product_brand': element['product_brand'],
                            'product_type': element['product_type'],
                            'product_size': element['product_size'],
                            'product_container': element['product_container'],
                            'retailprice': element['retailprice']
                        })
                )


    #########################################################
    #
    #       PRODUCT AVAILABILITY BY SUPPLIER
    ############################################################


    logging.info('Reading Product Availability data..')
    _psupp_colums = ['partkey','suppkey','availqty','supplycost','ps_comment']
    _psupp = f"{known_args.input}dss_partsupp.csv"
    ppsupp = (
                    p 
                        | 'Reading product Availability data' >> beam.io.ReadFromText(_psupp, skip_header_lines=1)
                        | 'Mapping product Availability data to Json' >> beam.ParDo(Split(columns=_psupp_colums))
                        | 'Creating Complex Key for Product and Supplier' >> beam.Map(lambda element:{
                                                                'ckey': "{}|{}".format(element['partkey'], element['suppkey']),
                                                                'availqty': element['availqty'],
                                                                'supplycost': element['supplycost']
            })
                )

    

    #########################################################
    #
    #       ITEMS DATA
    ############################################################

    logging.info('Reading items data..')
    _items_colums = ['orderkey','partkey','suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode','l_comment']
    _items = f"{known_args.input}dss_lineitem.csv"
    pitems = (
                    p 
                        | 'Reading items data' >> beam.io.ReadFromText(_items, skip_header_lines=1)
                        | 'Reshuffling items data to be parallel' >> beam.Reshuffle()
                        | 'Mapping items data to Json' >> beam.ParDo(Split(columns=_items_colums))
                        | 'Mapping items fields' >> beam.Map(lambda element: {
                                        'ckey': "{}|{}".format(element['partkey'], element['suppkey']),
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
                                        'shipdate': datetime.strptime(element['l_shipdate'],'"%Y-%m-%d"').strftime('%Y-%m-%d'),
                                        'commitdate': datetime.strptime(element['l_commitdate'],'"%Y-%m-%d"').strftime('%Y-%m-%d'),
                                        'receiptdate': datetime.strptime(element['l_receiptdate'],'"%Y-%m-%d"').strftime('%Y-%m-%d'),
                                        'delay' : (datetime.strptime(element['l_commitdate'], '"%Y-%m-%d"') - datetime.strptime(element['l_receiptdate'], '"%Y-%m-%d"')).days,
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