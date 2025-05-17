# Databricks notebook source
# MAGIC %md
# MAGIC #####This notebook contains script to insert config values like database name, password, server name into the configurations table.
# MAGIC **Here are the steps followed how configs are inserted:**
# MAGIC - Creating Widgets to get the parameters which needs to be inserted in config table.
# MAGIC - Getting widgets input as a parameter.
# MAGIC - Calling **commonConfigs()** class to use the addlog function to insert logs generated from this notebook in log table.
# MAGIC - Creating method **getUpdatedTablesList()** to get the updated tables to be inserted into the config.
# MAGIC - The method **InsertConfigValues()** contains logic to insert config values into the configurations table, it takes configvalues as a parameter.
# MAGIC - Calling necessary methods to get the config inserted into the config table.

# COMMAND ----------

# MAGIC %run "/deployments/dataimport/sql/audit/Log"

# COMMAND ----------

# MAGIC %run "/deployments/common/libraries/dbs_client"

# COMMAND ----------

# MAGIC %run "/deployments/dataimport/partners/sharestory/loads/rxrefill_loadsharestory_CommonConfigs"

# COMMAND ----------

#Importing necessary libraries
import collections
from datetime import datetime

# COMMAND ----------

# Creating widgets.
dbutils.widgets.text("DatabaseConfigName",'','Enter Single Variable') #eg. if projectname=reliefvet, DatabaseConfigName=reliefvet_db_name
dbutils.widgets.text("DatabaseName",'','Enter Single Variable') #valid database name
dbutils.widgets.text("TableConfigName",'','Enter Single Variable') #eg. if projectname=reliefvet, TableConfigName=reliefvet_tables
dbutils.widgets.text("TablesName",'','Enter Comma Separated List of Table Names') #valid tables name in this format - Hospital,Status,HospitalType
dbutils.widgets.text("WatermarkConfigName",'','Enter Single Variable') #eg. if projectname=reliefvet, WatermarkConfigName=reliefvet_hospitaltype_watermarks
dbutils.widgets.text("WatermarksName",'','Enter Comma Separated List of Table Names') #valid column names in this format - CreatedDate, LastModifiedDate

# COMMAND ----------

# Getting Widgets values as a parameter.
database_config_name=dbutils.widgets.get("DatabaseConfigName")
database_name=dbutils.widgets.get("DatabaseName")
table_config_name=dbutils.widgets.get("TableConfigName")
tables_name=dbutils.widgets.get("TablesName")
watermark_config_name=dbutils.widgets.get("WatermarkConfigName")
watermarks_name=dbutils.widgets.get("WatermarksName")

# COMMAND ----------

#Getting Configs from the notebook rxrefill_loadsharestory_CommonConfigs having class commonConfigs
out = {'notebook':'DCX_Common_InsertPostgresConfigValuesNotebook', 'result':'ERROR: '}
commonConfigs=commonConfigs(Notebook = 'DCX_Common_InsertPostgresConfigValuesNotebook')
#Assigning variables
loglist=[]
tablesAlreadyAvailable=[]
qry=f"""SELECT config_name, config_value
        FROM configurations.configvalues
        WHERE group_name IN('dcx_postgresql_watermark_settings','dcx_postgresql_db_settings','dcx_postgresql_table_settings') and config_name IN('{watermark_config_name}','{database_config_name}','{table_config_name}') AND is_active=true
      """
config=spark.sql(qry)
watermarksAlreadyAvailable=config.select('config_value').where(f"config_name='{watermark_config_name}'").collect()
databaseAlreadyExists=config.select('config_value').where(f"config_name='{database_config_name}'").collect()
tablesAlreadyAvailable=config.select('config_value').where(f"config_name='{table_config_name}'").collect()
qry="""SELECT config_name, config_value
        FROM configurations.configvalues AS c 
        WHERE c.group_name IN('dcx_postgresql_table_settings')"""
tbls=spark.sql(qry) 
tbls_details=tbls.rdd.collectAsMap()
postgresTablesLists = [item for v in tbls_details.values() for item in v.split(',')]

# COMMAND ----------

#Method to get the updated tables list or new tables list to insert it into the config.
def getUpdatedTablesList(table_config_name,tablesAlreadyAvailable):
  task=table_config_name.split('_')[0]
  newTablesName=[]
  if tablesAlreadyAvailable:
    tablesAlreadyAvailable=tablesAlreadyAvailable[0][0].split(',')
  widgetsTables=[tbls.strip() for tbls in tables_name.split(',')]
  for new_tbl in widgetsTables:
    if new_tbl.split('.')[-1] in postgresTablesLists:
      newTablesName.append(f"{task}_{new_tbl}")
    else:
      newTablesName.append(new_tbl)
  for tbl in newTablesName:
    tablesAlreadyAvailable.append(tbl)
  addedNewTables=",".join(tablesAlreadyAvailable)
  return addedNewTables

# COMMAND ----------

#Method to get the Watermark columns for the tables.
def getWatermarkCols():
  watermark_cols=[]
  get_watermarks_cols=[cols.strip() for cols in watermarks_name.split(',')]
  for col in get_watermarks_cols:
    watermark_cols.append(col)
  watermark_cols=",".join(watermark_cols)
  return watermark_cols

# COMMAND ----------

#This function inserts the config values provided as a parameter to the config table.
def InsertConfigValues(**configvalues):
  configvalue= list(configvalues.values())
  configuration = collections.namedtuple("ConfigDetails", ["config_name", "config_value", "IsEncrypt", "group_name"])
  config_details = [
    configuration(config_name = configvalue[0], config_value = configvalue[1], IsEncrypt = configvalue[2], group_name = configvalue[3])     
  ] 
  insert_configvalues_path = "/deployments/dataimport/sql/configurations/insert_configvalues"
  for config in config_details:
    qry = f"SELECT COUNT(*) AS cnt FROM configurations.configvalues WHERE LOWER(group_name) = LOWER('{config.group_name}') AND LOWER(config_name)= LOWER('{config.config_name}')"
    count = spark.sql(qry).first()["cnt"]
    if count > 0:
      loglist.append(commonConfigs.AddLog(f"{config.config_name} already exists in config",0))
    else:
      pipeline_params = {
        "config_name": config.config_name,
        "config_value": config.config_value,
        "IsEncrypt": config.IsEncrypt,
        "group_name": config.group_name
      }
      nb_results = dbutils.notebook.run(insert_configvalues_path, 0, pipeline_params)
      loglist.append(commonConfigs.AddLog(f"{config.config_name} has been inserted into config",0))

# COMMAND ----------

try:
  tablesToUpdateInConfig=getUpdatedTablesList(table_config_name,tablesAlreadyAvailable)
  if not databaseAlreadyExists and database_config_name and database_name:
    InsertConfigValues(config_name = database_config_name, config_value = database_name,IsEncrypt = "No", group_name = "dcx_postgresql_db_settings")
    InsertConfigValues(config_name = table_config_name, config_value = tablesToUpdateInConfig,IsEncrypt = "No", group_name = "dcx_postgresql_table_settings")
  elif table_config_name and tablesToUpdateInConfig:
    qry=f"""UPDATE configurations.configvalues
         SET config_value='{tablesToUpdateInConfig}'
         WHERE group_name='dcx_postgresql_table_settings' and config_name='{table_config_name}'
        """
    spark.sql(qry)
    loglist.append(commonConfigs.AddLog(f"Tables {tablesToUpdateInConfig} have been added to config {table_config_name}",0))
  else:
    pass
  watermarksToUpdateInConfig=getWatermarkCols()
  if not watermarksAlreadyAvailable and watermark_config_name and watermarks_name:
    InsertConfigValues(config_name = watermark_config_name, config_value = watermarksToUpdateInConfig, IsEncrypt = "No", group_name = "dcx_postgresql_watermark_settings")
  InsertLogs(str(loglist))
except Exception as e:
  out['result'] = 'ERROR: ' + str(e)
  loglist.append(commonConfigs.AddLog('ERROR: ' + str(e)[0:5000],0))
  InsertLogs(str(loglist))
  dbutils.notebook.exit(out)

# COMMAND ----------

out['result'] = 'SUCCESS'
dbutils.notebook.exit(out)
