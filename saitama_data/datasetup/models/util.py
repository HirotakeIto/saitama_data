import pandas as pd
# from pandas_schema import Column, Schema
# from pandas_schema.validation import CanConvertValidation

def get_schema_str(df: pd.DataFrame):
    def helper_type_str(string: str):
        if string.count('int'):
            return 'int'
        elif string.count('float'):
            return 'float'
        elif string.count('bool'):
            return 'bool'
        else:
            return 'str'
    # schema = []
    # for col, dtyp in zip(df.dtypes.index.tolist(), df.dtypes.tolist()):
    #     schema.append(Column(col, [CanConvertValidation(dtyp.type)]))
    schema_item_list = []
    column_type_item_list = []
    for col, dtyp in zip(df.dtypes.index.tolist(), df.dtypes.tolist()):
        schema_item_list.append('    Column(\"{col}\", [CanConvertValidation({typ})])'.format(col=col, typ=helper_type_str(dtyp.name)))
        # schema_item_list.append('    Column(\"{col}\", [])'.format(col=col, typ=helper_type_str(dtyp.name)))
        column_type_item_list.append('    \"{col}\": {typ}'.format(col=col, typ=helper_type_str(dtyp.name)))
    schema_string = 'schema = Schema([\n{item_list}\n])'.format(item_list = ',\n'.join(schema_item_list))
    column_type_string = "column_type = {{\n{item_list} \n}}".format(item_list=',\n'.join(column_type_item_list))
    print(column_type_string)
    print(schema_string)




