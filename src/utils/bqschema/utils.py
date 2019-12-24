import logging


def get_schema():
    data = {colum['name']:colum['type'] for colum in __SCHEMA}
    schema = '{"fields":' + json.dumps(__SCHEMA) + '}'
    return parse_table_schema_from_json(schema)


def _convert_date(row):
    datefields = []
    floatfields = []

    for fields in __SCHEMA:
        if fields['type'] == 'DATETIME':
            datefields.append(fields['name'])
        elif fields['type'] == 'FLOAT':
            floatfields.append(fields['name'])
    
    for datefield in datefields:
        row[datefield] = row[datefield].strftime("%Y-%m-%d %H:%M:%S.%f")
    
    for floatfield in floatfields:
        row[floatfield] = float(row[floatfield])
   
    return row