# Databricks notebook source
# MAGIC %md
# MAGIC ### Utility to Publish the Meta Data of all the Databases and Tables <br>within a Hive Meta Store.
# MAGIC 
# MAGIC #### Tables being Written are:
# MAGIC * sys.db_tables
# MAGIC * sys.db_table_columns
# MAGIC * sys.db_table_properties
# MAGIC * sys.db_table_partitions

# COMMAND ----------

try:
  db_list = dbutils.widgets.get("db_list")
except Exception as e:
  print("DB List Not Provided")
  db_list = None
if db_list and db_list != "None":
  print("Getting the List of DB(s) from Caller")
  db_list = db_list.split("|")
else:
  print("Generating the List of DB(s)")
  db_list = list()
  db_df = spark.sql("show databases")
  for row in db_df.collect() :
    db_list.append(row.databaseName)
print("DB List: \n{}".format(db_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Core Processing

# COMMAND ----------

all_info = dict()
for db in db_list:
  all_info[db] = dict()
  tbl_list = sqlContext.tableNames(dbName=db)
  tbl_list.sort()
  for tbl in tbl_list:
    print("Procesing DB: {},\tTable: {}".format(db, tbl))
    all_info[db][tbl] = dict()
    all_info[db][tbl]["meta_info"] = None
    all_info[db][tbl]["column_info"] = None
    all_info[db][tbl]["partition_info"] = None
    try:
      extended_data = list()
      try:
        tdf_detail = spark.sql("desc detail {db}.{tbl_name}".format(db=db, tbl_name=tbl))
        extended_row = tdf_detail.take(1)[0]
        extended_data.append(("partitionColumns", extended_row.partitionColumns, None))
        extended_data.append(("numFiles", extended_row.numFiles, None))
        extended_data.append(("sizeInBytes", extended_row.sizeInBytes, None))
      except Exception as e:
        print("Unable to Run DESC DETAIL on : [{db}.{tbl_name}]".format(db=db, tbl_name=tbl))
        print("Error : {}".format(e))
      tdf_extended = spark.sql("desc extended {db}.{tbl_name}".format(db=db, tbl_name=tbl))
      all_data = [(x.col_name, x.data_type, x.comment) for x in tdf_extended.collect()]
      all_cols = [x.col_name for x in tdf_extended.collect()]
      assert len(all_cols) == len(all_data)

      if '# Detailed Table Information' in all_cols:
        meta_index = all_cols.index("# Detailed Table Information")
        meta_cols = all_data[meta_index + 1 : ]
        meta_cols.extend(extended_data)

      all_info[db][tbl]["meta_info"] = meta_cols

      if "# Partition Information" in all_cols and "# col_name" in all_cols:
        part_info_index = all_cols.index("# Partition Information")
        col_name_index = all_cols.index("# col_name")
        part_cols = all_data[col_name_index + 1 : meta_index - 1]
        all_info[db][tbl]["partition_info"] = part_cols
        table_cols = all_data[0: part_info_index]
      else:
        table_cols = all_data[0: meta_index -1 ]

      all_info[db][tbl]["column_info"] = table_cols
    except Exception as e:
      print(e)
      print("Failed to Get Meta Data for the DB: {},\tTable: {}".format(db, tbl))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the List of Rows for Each Table

# COMMAND ----------

db_table_rows = list()
db_table_columns_rows = list()
db_table_properties_rows = list()
db_table_partitions_rows = list()

for db in all_info:
  for tbl in all_info[db]:
    db_table_rows.append((db, tbl, "{}.{}".format(db,tbl)))
    if all_info[db][tbl]['column_info']:
      for col_info in all_info[db][tbl]['column_info']:
        col_name, col_type, col_comment = col_info
        db_table_columns_rows.append((db, tbl, col_name, col_type, col_comment))
    if all_info[db][tbl]['partition_info']:
      for part_col_info in all_info[db][tbl]['partition_info']:
        part_col_name, part_col_type, _ = part_col_info
        db_table_partitions_rows.append((db, tbl, part_col_name, part_col_type))
    if all_info[db][tbl]['meta_info']:
      for meta_col_info in all_info[db][tbl]['meta_info']:
        meta_col_name, meta_col_value, _ = meta_col_info
        db_table_properties_rows.append((db, tbl, meta_col_name, meta_col_value))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Schema for the Dataframes

# COMMAND ----------

from pyspark.sql.types import *
schema_db_table_rows = StructType([StructField("db_name", StringType()),
                                  StructField("table_name", StringType()),
                                  StructField("table_name_fully_qualified", StringType())]
                                 )

schema_db_table_column_rows = StructType([StructField("db_name", StringType()),
                                  StructField("table_name", StringType()),
                                  StructField("column_name", StringType()),
                                  StructField("column_type", StringType()),
                                  StructField("column_comment", StringType())]
                                 )
schema_db_table_properties_rows = StructType([StructField("db_name", StringType()),
                                  StructField("table_name", StringType()),
                                  StructField("property_name", StringType()),
                                  StructField("property_value", StringType())]
                                 )

schema_db_table_partitions_rows = StructType([StructField("db_name", StringType()),
                                  StructField("table_name", StringType()),
                                  StructField("partition_column_name", StringType()),
                                  StructField("partition_column_type", StringType())]
                                 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a DataFrame

# COMMAND ----------

df_db_table_rows = spark.createDataFrame(db_table_rows, schema=schema_db_table_rows)
df_db_table_columns_rows = spark.createDataFrame(db_table_columns_rows, schema=schema_db_table_column_rows)
df_db_table_partitions_rows = spark.createDataFrame(db_table_partitions_rows, schema=schema_db_table_partitions_rows)
df_db_table_properties_rows = spark.createDataFrame(db_table_properties_rows, schema=schema_db_table_properties_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Friendly Statistics Check

# COMMAND ----------

print("Total Databases : {}".format(len(db_list)))
print("Total Tables : {}".format(len(tbl_list)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dump the Relevant Data into the "sys.db_tables"

# COMMAND ----------

try:
  df_db_table_rows.write.insertInto("sys.db_tables", overwrite=True)
except Exception as e:
  print("Failed to Write the Data into the Table")
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dump the Relevant Data into the "sys.db_table_columns"

# COMMAND ----------

try:
  df_db_table_columns_rows.write.insertInto("sys.db_table_columns", overwrite=True)
except Exception as e:
  print("Failed to Write the Data into the Table")
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dump the Relevant Data into the "sys.db_table_partitions"

# COMMAND ----------

try:
  df_db_table_partitions_rows.write.insertInto("sys.db_table_partitions", overwrite=True)
except Exception as e:
  print("Failed to Write the Data into the Table")
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dump the Relevant Data into the "sys.db_table_properties"

# COMMAND ----------

try:
  df_db_table_properties_rows.write.insertInto("sys.db_table_properties", overwrite=True)
except Exception as e:
  print("Failed to Write the Data into the Table")
  print(e)
  
