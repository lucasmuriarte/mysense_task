### BELOW IS A STARTER TEMPLATE AND WORKING CODE THAT READS DATA FROM INOUT AND OUTPUTS only the fields: cid, type and heartrates (already in json format) ###

### MODIFY THE CODE BELOW TO COMPLETE THE TASK A and B ###

import os

from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()

table_env = StreamTableEnvironment.create(stream_execution_environment=env)


def get_input_table_ddl(input_table_name):
    return  """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 ts BIGINT,
                 heartrates ARRAY<ROW<heartrate INT, ts BIGINT, `type` STRING>>,
                 model STRING
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/input/events.json',
            'format' = 'json'
            )""".format(input_table_name)


### Output table only for example purposes ###
def get_output_table_ddl(output_table_name):
    return   """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 ts BIGINT,
                 heartrates ARRAY<ROW<heartrate INT, ts BIGINT, `type` STRING>>,
                 model STRING
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/starter_result',
            'format' = 'json'
            )""".format(output_table_name)



def main():

    # tables names
    input_table_name = "input_heart_rate_data"
    output_table_name = "transformed_heart_rate_data"

    ### drop tables if they exist

    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name))

    # create the tables for input and output by executing the DDL
    table_env.execute_sql(get_input_table_ddl(input_table_name))
    table_env.execute_sql(get_output_table_ddl(output_table_name))

    # Read from table to begin transformations
    tab1 = table_env.from_path(input_table_name)

    
    # Insert table  
    final_result = tab1.select(col("*")).execute_insert(output_table_name)

    final_result.wait()

if __name__ == "__main__":
    main()
