### BELOW IS A STARTER TEMPLATE AND WORKING CODE THAT READS DATA FROM INOUT AND OUTPUTS only the fields: cid, type and heartrates (already in json format) ###

### MODIFY THE CODE BELOW TO COMPLETE THE TASK A and B ###
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

from pyflink.table import DataTypes
from pyflink.table.udf import udtf
from input_output_tables import (get_input_table_ddl_task_A, get_input_table_ddl_task_B, 
                                 get_output_table_ddl_task_A, get_output_table_ddl_task_B)
from udf_helpers import row_selection, RowTimeExpansion
import argparse


env = StreamExecutionEnvironment.get_execution_environment()

table_env = StreamTableEnvironment.create(stream_execution_environment=env)
    

def run_taskA(path: str):
    """
    Paratmeters:
    -----------

    Path: Str
        Path to the folder containing the data
    """
    # tables names
    input_table_name_taskA = "input_heart_rate_data"
    output_table_name_taskA = "output_table_name_taskA"

    ### drop tables if they exist
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name_taskA))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name_taskA))

    # create the tables for input and output by executing the DDL
    table_env.execute_sql(get_input_table_ddl_task_A(input_table_name_taskA, path))
    table_env.execute_sql(get_output_table_ddl_task_A(output_table_name_taskA, path))

    # Read from table to begin transformations
    raw_data = table_env.from_path(input_table_name_taskA)
    row_expansion = udtf(RowTimeExpansion('UTC'), result_types=[DataTypes.STRING(), DataTypes.STRING(),
                                                                DataTypes.BIGINT(), DataTypes.STRING()])

    result_taksA = raw_data.filter(row_selection(col("cid"), col("heartrates")))\
        .flat_map(row_expansion(col("cid"), col("heartrates"))).alias("cid", "type", "heartrate", "hr_time")

    result_taksA.execute_insert(output_table_name_taskA).wait()


def run_taskB(path: str):
    """
    Paratmeters:
    -----------

    Path: Str
        Path to the folder containing the data
    """
    ####### START TASK B
    input_table_name_taskB = "input_inter_table"
    output_table_name_taskB = "output_table_name_taskB"

    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name_taskB))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name_taskB))

    table_env.execute_sql(get_input_table_ddl_task_B(input_table_name_taskB, path))
    table_env.execute_sql(get_output_table_ddl_task_B(output_table_name_taskB, path))
        
    # Read from table to begin transformations
    tab2 = table_env.from_path(input_table_name_taskB)

    result = tab2.window(Tumble.over(lit(10).minute).on(col("hr_time")).alias("time")) \
    .group_by(col("time"), col("cid"))\
    .select(col('cid'), col('time').start.alias('time_frame_start'), col("heartrate").avg.alias("average_heart_rate")) 
    
    result.execute_insert(output_table_name_taskB).wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CLI Example')
    parser.add_argument('--data-path', nargs='?', default="/opt/heart_rate_flink", help='path to the data folder')
    args = parser.parse_args()
    run_taskA(args.data_path)
    run_taskB(args.data_path)