# BASE_PATH = "/home/lucas/dev/docker/ds-deng-tasks-main/"
BASE_PATH = "/opt/heart_rate_flink"
# Input table of original source
def get_input_table_ddl_task_A(input_table_name, path=None):
    if not path:
        path = BASE_PATH 
    return  """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 ts BIGINT,
                 heartrates ARRAY<ROW<heartrate INT, ts BIGINT, `type` STRING>>,
                 model STRING
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{1}/data/input/events.json',
            'format' = 'json'
            )""".format(input_table_name, path)


# Output table that can be used for Task A
def get_output_table_ddl_task_A(output_table, path=None):
    if not path:
        path = BASE_PATH 
    return  """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 heartrate BIGINT,
                 hr_time STRING
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{1}/data/output/task_a_result/',
            'format' = 'json'
            )""".format(output_table, path)


# Input table that can be used for Task B
def get_input_table_ddl_task_B(input_table_name, path=None):
    if not path:
        path = BASE_PATH 
    return  """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 heartrate BIGINT,
                 hr_time TIMESTAMP(3),
                 WATERMARK FOR hr_time AS hr_time - INTERVAL '1' MINUTE
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{1}/data/output/task_a_result/',
            'format' = 'json'
            )""".format(input_table_name, path)


# Output table that can be used for Task B
def get_output_table_ddl_task_B(output_table, path=None):
    if not path:
        path = BASE_PATH 
    return  """CREATE TABLE {0} (
                cid STRING ,         
                time_frame_start TIMESTAMP(3),
                average_heart_rate BIGINT   
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{1}/data/output/task_b_result/',
            'format' = 'json'
            )""".format(output_table, path)