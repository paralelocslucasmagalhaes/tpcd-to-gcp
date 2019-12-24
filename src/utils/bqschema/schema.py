__COLUMNS = ['po', 'total', 'customer', 'status', 'modification_date', 'due_date', 'created_date', 'discount_percent', 'discount_value']

__SCHEMA = [
    {
      "description": "orderkey",
      "mode": "REQUIRED",
      "name": "orderkey",
      "type": "FLOAT"
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
    },
    {
        "description": "customer_name",
        "mode": "NULLABLE",
        "name": "customer_name",
        "type": "STRING"
    },

    {
        "description": "customer_addres",
        "mode": "NULLABLE",
        "name": "customer_addres",
        "type": "STRING"
    },
    {
        "description": "mktsegment",
        "mode": "NULLABLE",
        "name": "mktsegment",
        "type": "STRING"
    },
    {
        "description": "customer_nation",
        "mode": "NULLABLE",
        "name": "customer_nation",
        "type": "STRING"
    },
    {
        "description": "customer_region",
        "mode": "NULLABLE",
        "name": "customer_region",
        "type": "STRING"
    },
    {
        "description": "ITEMS",
        "mode": "REPEATED",
        "name": "items",
        "type": "RECORD",
        "fields":[
            {
                "description": "linenumber",
                "mode": "NULLABLE",
                "name": "linenumber",
                "type": "INTEGER"
            },
            {
                "description": "quantity",
                "mode": "NULLABLE",
                "name": "quantity",
                "type": "FLOAT"
            },
            {
                "description": "extendedprice",
                "mode": "NULLABLE",
                "name": "extendedprice",
                "type": "FLOAT"
            },


            {
                "description": "discount",
                "mode": "NULLABLE",
                "name": "discount",
                "type": "FLOAT"
            },
            {
                "description": "tax",
                "mode": "NULLABLE",
                "name": "tax",
                "type": "FLOAT"
            },
            {
                "description": "returnflag",
                "mode": "NULLABLE",
                "name": "returnflag",
                "type": "STRING"
            },
            {
                "description": "linestatus",
                "mode": "NULLABLE",
                "name": "linestatus",
                "type": "STRING"
            },
            {
                "description": "shipdate",
                "mode": "NULLABLE",
                "name": "shipdate",
                "type": "DATE"
            },
            {
                "description": "commitdate",
                "mode": "NULLABLE",
                "name": "commitdate",
                "type": "DATE"
            },

            {
                "description": "receiptdate",
                "mode": "NULLABLE",
                "name": "receiptdate",
                "type": "DATE"
            },

            {
                "description": "delay",
                "mode": "NULLABLE",
                "name": "delay",
                "type": "INTEGER"
            },

            {
                "description": "shipinstruct",
                "mode": "NULLABLE",
                "name": "shipinstruct",
                "type": "STRING"
            },
            {
                "description": "shipmode",
                "mode": "NULLABLE",
                "name": "shipmode",
                "type": "STRING"
            },
            {
                "description": "product_name",
                "mode": "NULLABLE",
                "name": "product_name",
                "type": "STRING"
            },
            {
                "description": "product_manufacture",
                "mode": "NULLABLE",
                "name": "product_manufacture",
                "type": "STRING"
            },

            {
                "description": "product_brand",
                "mode": "NULLABLE",
                "name": "product_brand",
                "type": "STRING"
            },
            {
                "description": "product_type",
                "mode": "NULLABLE",
                "name": "product_type",
                "type": "STRING"
            },
            {
                "description": "product_size",
                "mode": "NULLABLE",
                "name": "product_size",
                "type": "INTEGER"
            },
            {
                "description": "product_container",
                "mode": "NULLABLE",
                "name": "product_container",
                "type": "STRING"
            },
            {
                "description": "retailprice",
                "mode": "NULLABLE",
                "name": "retailprice",
                "type": "FLOAT"
            },
            {
                "description": "supplier_name",
                "mode": "NULLABLE",
                "name": "supplier_name",
                "type": "STRING"
            },
            {
                "description": "supplier_address",
                "mode": "NULLABLE",
                "name": "supplier_address",
                "type": "STRING"
            },
            {
                "description": "supplier_nation",
                "mode": "NULLABLE",
                "name": "supplier_nation",
                "type": "STRING"
            },

            {
                "description": "supplier_region",
                "mode": "NULLABLE",
                "name": "supplier_region",
                "type": "STRING"
            },
            {
                "description": "availqty",
                "mode": "NULLABLE",
                "name": "availqty",
                "type": "FLOAT"
            },
            {
                "description": "supplycost",
                "mode": "NULLABLE",
                "name": "supplycost",
                "type": "FLOAT"
            }
        ]
    },



  ]

_FIELDS = dict(fields = __SCHEMA)