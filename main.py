from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import concat

# preparing environment. In batch mode.
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env_batch = TableEnvironment.create(env_settings)

table_student = table_env_batch.from_elements(
    [(1, 'M', 'Zhang', 'San'), (2, 'F', 'Li', 'Si'), (3, 'M', 'Wang', 'Wu')],
    ['id', 'gender', 'surname', 'givenname']
)
table_student.print_schema()
table_env_batch.create_temporary_view('temp_view', table_student)

table_env_batch.execute_sql("select * from temp_view").print()
# 按性别...
table_env_batch.execute_sql('select gender,count(id),as cnt from temp_view group by gender')

# select data
# getting data from the cluster
df = table_student.to_pandas()
print(df)
# generate DAG
# table_select = table_student.select("id,gender")
# table_select = table_student.select(table_student.id, table_student.gender,
#                                    concat(table_student.surname, '-', table_student.givenname)
#                                    .alias('fullname'))
# table_select = table_student.select("id,gender,surname+'-'+givenname as fullname")
table_select = table_student.select(table_student.id, concat(table_student.surname, '-', table_student.givenname)
                                    .alias('fullname'))
# run it. Actually.
df = table_select.to_pandas()
print(df)
