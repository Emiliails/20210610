"""
(1) 使用PyFlink 的Table API编写一个pyflink程序；
(2) 从随机数据源datagen中生成学生信息：id（1-100），name（3位长度），age（5～40）。
(3) 使用table api进行流式处理，将年龄属于18～25之间的学生信息，以csv格式保存到文件系统中。
"""

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import concat

# 配置环境，流模式
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# 创建环境
table_env = TableEnvironment.create(env_settings)

# 生成随机数据流，三个引号无需转义
table_env.execute_sql("""
    create table random_source(
    id int,
    age int,
    name string,
    ts as localtimestamp
    ) with (
        'connector' = 'datagen',
        'rows-per-second' = '2',
        'fields.id.start' = '1',
        'fields.id.end' = '100',
        'fields.id.kind' = 'sequence',
        'fields.name.length' = '2'
    )
"""
                      )

# table_env.execute_sql('select * from random_source').print()

# 流式分组查询
# table_env.execute_sql('select name,count(id),sum(age) from random_source group by name').print()

# 每秒过来的数据分组查询
# table_env.execute_sql("""
# select name,count(id),sum(age)
#     from table(tumble(table random_source,descriptor(ts),interval '1' seconds))
#     group by name
# """).print()

# 写为csv文件
table_env.execute_sql("""
    create table file_sink(
        id int,
        age int,
        name string
    ) with (
        'connector' = 'filesystem',
        'path' = '/Users/niubo/PycharmProjects/20210610/',
        'format' = 'csv'
    )
""")

table_env.execute_sql("""
    insert into file_sink
        select  id,age/2,name from random_source
        where id>20
""").wait()
