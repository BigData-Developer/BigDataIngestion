# Databricks notebook source
# MAGIC %md
# MAGIC #####Common Notebook to load data from PostgreSql Server to the ADLS GEN2 
# MAGIC **Steps followed to achieve data load:**
# MAGIC - Assigning variables to be used in the script later.
# MAGIC - Getting secret key from key vault to decrypt the password used while connecting to PostgreSql Server.
# MAGIC - Getting list of databases and tables available in the respective databases from config table.
# MAGIC - Getting other configurations like Server name, userid, databasename from the **configurations.configvalues** table and decoding the encoded password using base64.
# MAGIC - Defining a method **getFilePath()** to get the variable file path as per the table name.
# MAGIC - Defining a method **getSourceData()** which connects to the PostgreSQL Server and create a dataframe on top of it.
# MAGIC - Defining a method **writeToDatalake()** to write the data from source to the ADLS GEN2 filepath.
# MAGIC - Main command where the list of databases are iterated followed by list of tables name for the respective databases are iterated to read and write the data from source to destination.

# COMMAND ----------

#Importing libraries
from datetime import datetime,timezone,timedelta 
from dateutil import parser
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import base64
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# COMMAND ----------

# DBTITLE 1,This Notebook contains functions to insert logs to the log table.
# MAGIC %run "/deployments/dataimport/sql/audit/Log"

# COMMAND ----------

# DBTITLE 1,This Noteboook contains functions to get databases name with their corresponding name for eg. birepository_db(variable to get db name) = birepository(db name).
# MAGIC %run "/deployments/dataprocessing/databases/cdw/loads/CDWConfig"

# COMMAND ----------

# DBTITLE 1,The notebook has functions to encrypt and decrypt the key provided, we used here to decrypt the encrypted password.
# MAGIC %run "/deployments/dataprocessing/datasecurity/cryptology"

# COMMAND ----------

# DBTITLE 1,The notebook has functions to add log to a list, get other configs used in multiple notebooks so that repetitive code in multiple notebooks avoided.
# MAGIC %run "/deployments/dataimport/partners/sharestory/loads/rxrefill_loadsharestory_CommonConfigs"

# COMMAND ----------

#Assigning parameters to the botebook.
out = {'notebook':'DCX_Common_PostgresToDataLake_LoadNotebook', 'result':'ERROR: '}
dbutils.widgets.text('singlebatchdatalimit','1000000','Data  Limit for Single Batch Data Load')
singlebatchdatalimit=int(dbutils.widgets.get('singlebatchdatalimit'))
commonConfigs=commonConfigs(Notebook='DCX_Common_PostgresToDataLake_LoadNotebook')
loglist=[]

# COMMAND ----------

#############################################
#Getting Secret from Azure Key Vault Storage
#############################################
try:  
  #Adding logs to the loglist.
  loglist.append(commonConfigs.AddLog("Start :Get Scope Name ",0))
  #Query to get the scope_name from configurations table, the scope_name is used to get the secret from Azure Key Vault.
  qry = """
    SELECT config_value 
    FROM 
    configurations.configvalues AS c 
    WHERE c.group_name = 'keyvault settings' 
      AND c.config_name = 'config scopename' 
      AND c.is_active = TRUE 
    LIMIT 1 """
  scope_name = ""
  firstRow = spark.sql(qry).first()
  if firstRow is None:
    raise Exception("scope_name is not defined in configuration")
  scope_name= str(firstRow['config_value'])
  secret = dbutils.secrets.get(scope_name,'config')
except Exception as e:
  loglist.append(commonConfigs.AddLog('ERROR:Get Scope Name: ' + str(e)[0:5000], 0))
  InsertLogs(str(loglist))
  raise(e)

# COMMAND ----------

################################################################################################
#Getting list of Databases and  Tables from Configurations table to connect to PosgreSql Server 
################################################################################################
try:
  qry="""SELECT config_name, config_value
         FROM configurations.configvalues AS c 
         WHERE c.group_name IN('dcx_postgresql_db_settings') AND c.is_active = TRUE"""
  dbs=spark.sql(qry)
  dbs_details=dbs.rdd.collectAsMap()
  if not dbs_details:
    raise Exception("No databases detail available in the config table")  
  qry="""SELECT config_name, config_value
         FROM configurations.configvalues AS c 
         WHERE c.group_name IN('dcx_postgresql_table_settings') AND c.is_active = TRUE"""
  tbls=spark.sql(qry) 
  tbls_details=tbls.rdd.collectAsMap()
  if not tbls_details:
    raise Exception("No tables detail available in the config table")
except Exception as e:
  loglist.append(commonConfigs.AddLog('ERROR:Get configs: ' + str(e)[0:5000], 0))
  InsertLogs(str(loglist))
  raise(e)

# COMMAND ----------

#######################################################################################
#Getting Postgres Credentials from Configurations table to connect to PosgreSql Server 
#######################################################################################
try:  
  loglist.append(commonConfigs.AddLog("Start :GET VALUES FROM CONFIGURATIONS ",0))
  qry =f"""
    SELECT * FROM 
    (
    SELECT config_name, config_value
    FROM configurations.configvalues AS c 
    WHERE c.group_name IN('urgentcare settings') AND c.config_name IN('urgentcare server name','urgentcare userid','urgentcare filepath') 
    )
    UNION
    SELECT config_name,
    decrypt("{secret}", config_value) as config_value
    FROM configurations.configvalues AS c 
    WHERE c.group_name = 'urgentcare settings' AND c.config_name = 'urgentcare pass' AND c.is_active = TRUE
    """
  Conf = spark.sql(qry)
  isconfigavailable=Conf.select("config_value").where("config_value=''").count()
  if isconfigavailable>0:
    raise Exception("Auditlog writer values is not defined in configuration")
  sql_server=Conf.select("config_value").where("config_name='urgentcare server name'").collect()[0][0]
  w_uid =Conf.select("config_value").where("config_name='urgentcare userid'").collect()[0][0]
  password=Conf.select("config_value").where("config_name='urgentcare pass'").collect()[0][0]
  filepath=Conf.select("config_value").where("config_name='urgentcare filepath'").collect()[0][0]
  w_pass = str(base64.b64decode(password.encode('ascii')).decode("utf-8"))
except Exception as e:
  loglist.append(commonConfigs.AddLog('ERROR:Get Scope Name: ' + str(e)[0:5000], 0))
  InsertLogs(str(loglist))
  raise(e)

# COMMAND ----------

#Getting the watermark column from the config table to filter out the data based on watermark column for big volume data load.
def getCoalesceString():
  watermarkcols=[]
  qry=f"""SELECT config_value 
         FROM configurations.configvalues
         WHERE group_name='dcx_postgresql_watermark_settings' AND config_name='{tbl_config.split('_')[0].lower()}_{table.strip('"').lower()}_watermarks' AND is_active=True
      """
  firstRow=spark.sql(qry).first()
  if firstRow is not None:
    columns=firstRow['config_value']
    columns=columns.split(',')
    for cols in columns:
      watermarkcols.append(cols)
  else:
    columnDataTypes = stageDf.dtypes
    for columnName, dataType in columnDataTypes:
      if dataType=='timestamp' and (columnName.lower().startswith('created') or columnName.lower().startswith('modified') or columnName.lower().startswith('updated') or columnName.lower().startswith('lastmodified') or columnName.lower().startswith('log') or columnName.lower().startswith('registration')):
        watermarkcols.append(columnName)
      elif dataType=='date' and (columnName.lower().startswith('merge') or columnName.lower().startswith('unmerge')):
        watermarkcols.append(columnName)
      else: pass
  coalesceString=','.join(watermarkcols) 
  return coalesceString

# COMMAND ----------

#Method to get the correct FilePath as per the table name
def getFilePath():
  task=tbl_config.split('_')[0]
  if len(tbl.split("."))>1:
    if len(tbl.split(".")[0].split('__'))>1:
      schema_name=f'"{tbl.split(".")[0].split("__")[1]}"'
      file_path=f'{filepath}{task}{tbl.split(".")[-1].lower()}/'
    else:
      schema_name=f'"{tbl.split(".")[0]}"'
    table=f'"{tbl.split(".")[-1]}"'
    file_path=f'{filepath}{tbl.split(".")[-1]}/'
    selectAllQuery=f"SELECT * FROM {schema_name}.{table}"
    filterQueryTable=f'{schema_name}.{table}'
  elif len(tbl.split('__'))>1:
    table=f'"{tbl.split("__")[-1]}"'
    selectAllQuery=f"SELECT * FROM {table}"
    filterQueryTable=table
    if task=='rxrefill':
      file_path=f"{filepath}Rx{tbl.split('__')[-1]}/"
    else:
      file_path=f"{filepath}{task}{tbl.split('__')[-1].lower()}/"
  else:
    table=f'"{tbl}"'
    selectAllQuery=f"SELECT * FROM {table}"
    filterQueryTable=table
    file_path=f"{filepath}{tbl}/"
  return file_path,selectAllQuery,filterQueryTable,table

# COMMAND ----------

#Method to get the list of years, quarters and months in the given table's watermark column
def getListOfYearQuarterMonth():  
  listofyears=[]
  listofmonths=[]
  listofquarters=[]
  years=spark.sql(f"select distinct year(coalesce({coalesceString})) as year from tempview").collect()
  quarters=spark.sql(f"select distinct quarter(coalesce({coalesceString})) as quarter from tempview").collect()
  months=spark.sql(f"select distinct month(coalesce({coalesceString})) as month from tempview").collect()
  for yr in years:
    listofyears.append(yr.year)
  for qtr in quarters:
    listofquarters.append(qtr.quarter)
  for mth in months:
    listofmonths.append(mth.month)
  return listofyears,listofquarters,listofmonths

# COMMAND ----------

#Method to get the count of records a table contains for each year, quarter and month data
def getDataCountYearlyQuarterlyMonthly(**kwargs):
  monthsCountAboveLimit=[]
  monthsCountBelowLimit=[]
  daysCount=[]
  quarterCountAboveLimit=[]
  quarterCountBelowLimit=[]
  year=kwargs.get('year','')
  month=kwargs.get('month','')
  quarter=kwargs.get('quarter','')
  if year and quarter and not month:
    monthsCountAboveLimit=spark.sql(f"select month(coalesce({coalesceString})) as month, count(1) as count from tempview where year(coalesce({coalesceString}))={year} and quarter(coalesce({coalesceString}))={quarter} group by 1 having count>{singlebatchdatalimit}").collect()
    monthsCountBelowLimit=spark.sql(f"select month(coalesce({coalesceString})) as month, count(1) as count from tempview where year(coalesce({coalesceString}))={year} and quarter(coalesce({coalesceString}))={quarter} group by 1 having count<{singlebatchdatalimit}").collect()
  elif year and not month and not quarter:  
    quarterCountAboveLimit=spark.sql(f"select quarter(coalesce({coalesceString})) as quarter, count(1) as count from tempview where year(coalesce({coalesceString}))={year} group by 1 having count>{singlebatchdatalimit}").collect()
    quarterCountBelowLimit=spark.sql(f"select quarter(coalesce({coalesceString})) as quarter, count(1) as count from tempview where year(coalesce({coalesceString}))={year} group by 1 having count<{singlebatchdatalimit}").collect()
  elif year and quarter and month:
    daysCount=spark.sql(f"select day(coalesce({coalesceString})) as day, count(1) as count from tempview where year(coalesce({coalesceString}))={year} and quarter(coalesce({coalesceString}))={quarter} and month(coalesce({coalesceString}))={month} group by 1").collect()
  else:
    pass
  monthsCountAboveLimit=[x.month for x in monthsCountAboveLimit]
  monthsCountBelowLimit=[x.month for x in monthsCountBelowLimit]
  quarterCountAboveLimit=[x.quarter for x in quarterCountAboveLimit]
  quarterCountBelowLimit=[x.quarter for x in quarterCountBelowLimit]
  daysCount=[x.day for x in daysCount]
  return monthsCountAboveLimit,monthsCountBelowLimit,daysCount,quarterCountAboveLimit,quarterCountBelowLimit

# COMMAND ----------

#Method to get the filtered source data records as per the Year, Quarter and Month
def getFilteredSourceData(**kwargs):
  year=kwargs.get('year','')
  month=kwargs.get('month','')
  day=kwargs.get('day','')
  quarter=kwargs.get('quarter','')
  if year and not month and not day and not quarter:
    dataToWrite=spark.sql(f"select * from tempview where year(coalesce({coalesceString}))={year}")
  elif month and year and not day and not quarter:
    dataToWrite=spark.sql(f"select * from tempview where year(coalesce({coalesceString}))={year} and month(coalesce({coalesceString}))={month}")
  elif month and year and day and not quarter:
    dataToWrite=spark.sql(f"select * from tempview where year(coalesce({coalesceString}))={year} and month(coalesce({coalesceString}))={month} and day(coalesce({coalesceString}))={day}")
  elif quarter and year and not month and not day:
    dataToWrite=spark.sql(f"select * from tempview where year(coalesce({coalesceString}))={year} and quarter(coalesce({coalesceString}))={quarter}")
  else:
    dataToWrite=spark.sql(f"select * from tempview")
  return dataToWrite

# COMMAND ----------

#Method to get the QUERY which filters the source data based on watermark column if data volume is huge.
def getQryFilteredOnWatermark(table,lastloadDate):
  coalesceCol=[]
  coalesceStrCols=coalesceString.split(',')
  for col in coalesceStrCols:
    col=f'"{col}"'
    coalesceCol.append(col)
  coalesceColumns=','.join(coalesceCol)
  if len(coalesceString)>0:
    qry=f"SELECT * FROM {table} WHERE COALESCE({coalesceColumns}) >= CAST('{lastloadDate}' AS TIMESTAMP)"
  else:
    qry=f"SELECT * FROM {table}"
  return qry

# COMMAND ----------

def getSourceData(sql_db,query):
  stageDf =(spark.read
  .format("postgresql")
  .option("host", sql_server)
  .option("port", 5432)
  .option("database", sql_db)
  .option("query", query)
  .option("user", w_uid)
  .option("password", w_pass)
  .load())
  return stageDf

# COMMAND ----------

#Method to write the source data to a data lake filepath
def writeToDatalake(stageDf,tbl_config,file_path,mode):
  if tbl_config.split('_')[0] in ['reliefvet','appointmentwaitlist']:
    stageDf.coalesce(1).write.format('parquet').mode(f"{mode}").option("mergeSchema", "true").save(writefilepath)
  else:
    stageDf.coalesce(1).write.format('delta').mode(f"{mode}").option("mergeSchema", "true").save(writefilepath)

# COMMAND ----------

# DBTITLE 1,Method to load the Big data volume in chunks based on data count availability
#Method to load the Big data volume in chunks based on data count availability
#************************************************************************************************************************************#
  # Looping through the years, quarters, months and days to get the data to be written in the data lake.
  # If years count is above the limit, Quarter and Month are taken from the above list, else yearly  data  is written.
  # If quarter count is above the limit, Month is taken from the above list, else quarterly data is written.
  # If month count is above the limit, each days data is written, else monthly data is written.
#************************************************************************************************************************************#
def loadingBigDataInChunks():
  yearsCountAboveLimit=spark.sql(f"select year(coalesce({coalesceString})) as year, count(1) as count from tempview group by 1 having count>{singlebatchdatalimit}").collect()
  yearsCountAboveLimit=[x.year for x in yearsCountAboveLimit]
  for year in getListOfYearQuarterMonth()[0]:
    if year in yearsCountAboveLimit:
      for quarter in getListOfYearQuarterMonth()[1]:
        if quarter in getDataCountYearlyQuarterlyMonthly(year=year)[3]:
          for month in getListOfYearQuarterMonth()[2]:
            if month in getDataCountYearlyQuarterlyMonthly(year=year,quarter=quarter)[0]:
              for days in getDataCountYearlyQuarterlyMonthly(year=year,quarter=quarter,month=month)[2]:
                dataToWrite=getFilteredSourceData(year=year,month=month,day=days)
                writeToDatalake(dataToWrite,tbl_config,file_path,mode='append')
            elif month in getDataCountYearlyQuarterlyMonthly(year=year,quarter=quarter)[1]:
              dataToWrite=getFilteredSourceData(year=year,month=month)
              writeToDatalake(dataToWrite,tbl_config,file_path,mode='append')
            else: pass
        elif quarter in getDataCountYearlyQuarterlyMonthly(year=year)[4]:
          dataToWrite=getFilteredSourceData(year=year,quarter=quarter)
          writeToDatalake(dataToWrite,tbl_config,file_path,mode='append')
        else: pass
    else:
      dataToWrite=getFilteredSourceData(year=year)
      writeToDatalake(dataToWrite,tbl_config,file_path,mode='append')

# COMMAND ----------

#Method to get the MAX watermark time from the written data to update the lastloadDate Config
def getMaxDateToUpdateLastloaddate():
  if tbl_config.split('_')[0] in ['reliefvet','appointmentwaitlist']:
    spark.read.format("parquet").load(writefilepath).createOrReplaceTempView("writtendata")
  else:
    spark.read.format("delta").load(writefilepath).createOrReplaceTempView("writtendata")   
  qry=f"""SELECT DATEADD(hour,-80,maxdate) as loadDate 
          FROM 
          (
           SELECT MAX(COALESCE({coalesceString})) as maxdate 
           FROM writtendata
          )"""
  firstRow=spark.sql(qry).first()
  if firstRow['loadDate'] is not None:
    loadDate=firstRow['loadDate']
  else: 
    loadDate=lastloadDate
  return loadDate  

# COMMAND ----------

#Method to Insert or update the lastloadDate config to get the data filtered on this date during subsequent load
def updatingLastloaddateConfig(systemtype,insertconfig=False):
  loadDate=getMaxDateToUpdateLastloaddate()
  qry="SELECT MAX(tableloaddetailsid) from configurations.TableLoadDetails"
  max_key=spark.sql(qry).collect()[0][0]
  if max_key is None:
    max_key=0
  max_key=max_key+1
  qry = f"""
      MERGE INTO 
          configurations.TableLoadDetails AS T
      USING
       (
            SELECT 
              CAST('{max_key}' AS INT) AS tableloaddetailsid,
             '{systemtype}' AS SystemType,
             'sharestory' AS DatabaseName, 
             '{lastloadDateTable.lower()}' AS TableName, 
             CAST('{loadDate}' AS TIMESTAMP) AS LastLoadDate, 
             CURRENT_TIMESTAMP() AS SqlCreatedDate, 
             CURRENT_TIMESTAMP() AS SqlUpdatedDate
        ) S 
      ON T.SystemType = S.SystemType
      AND T.DatabaseName = S.DatabaseName
      AND lower(T.TableName) = lower(S.TableName)
      WHEN MATCHED THEN
      UPDATE SET
      T.LastLoadDate = S.LastLoadDate,      
      T.SqlUpdatedDate = S.SqlUpdatedDate
      WHEN NOT MATCHED AND {insertconfig} THEN 
      INSERT
      (
          tableloaddetailsid,
          SystemType,
          DatabaseName,
          TableName,
          LastLoadDate,
          SqlCreatedDate,
          SqlUpdatedDate
      )
      VALUES
      (
          S.tableloaddetailsid,
          S.SystemType,
          S.DatabaseName,
          S.TableName,
          S.LastLoadDate,
          S.SqlCreatedDate,
          NULL
      ) """
  spark.sql(qry)

# COMMAND ----------

#Method to read and write the data based on its Data volume
def readingSourceDataAndWritingToFilepath():
  stageDf.createOrReplaceTempView("tempview")
  if stageCount < singlebatchdatalimit:
    dataToWrite=getFilteredSourceData()
    writeToDatalake(dataToWrite,tbl_config,file_path,mode='overwrite')
    if len(coalesceString)>0:  
      updatingLastloaddateConfig(systemtype)
  else:
    if len(coalesceString)>0:
      print("loading BigData in chunks...")
      loadingBigDataInChunks() 
      updatingLastloaddateConfig(systemtype,insertconfig=True)
    else:
      print("Missing Watermark!! loading BigData in Bulk...")
      dataToWrite=getFilteredSourceData()
      writeToDatalake(dataToWrite,tbl_config,file_path,mode='overwrite')
      email_body='<p><b>WARNING!!</b> This is just a kind reminder to add the Watermark column for Table: '+table.strip('"')+'.<BR/>Since the table contains more than 1 million records, it is good to have Watermark column to avoid failure in future.</p>'
      dbutils.notebook.run('/deployments/housekeeping/EmailTemplate',0,{'ebody':f'{email_body}','ename':'import_dcx_postgresdata Job Warning Notification',"job_outputs":"{}"})

# COMMAND ----------

#Main
try:
  todaydate=datetime.now(timezone.utc)
  rundatetoday = str(todaydate.strftime('%Y-%m-%d'))
  year_, month_, day_ = rundatetoday.split('-')
  for dbs_config,dbs_name in dbs_details.items():
    print(f"<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>> {dbs_config.split('_')[0].capitalize()} <<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>")
    for tbl_config,tbl_name in tbls_details.items():
      tbl_name=tbl_name.split(',')
      if dbs_config.split('_')[0]==tbl_config.split('_')[0]:
        for tbl in tbl_name:
          file_path,selectAllQuery,filterQueryTable,table=getFilePath()[0],getFilePath()[1],getFilePath()[2],getFilePath()[3]
          lastloadDateTable=table.strip('"')
          if tbl_config.split('_')[0]=='scribe' and tbl=='AuditLog':
            systemtype='databricks'
            lastloadDate=GetMaxTimestampUsingPython(f'{systemtype}','sharestory',lastloadDateTable.lower())
          else:
            systemtype='databricks_prod_new_workspace'
            lastloadDate=GetMaxTimestampUsingPython(f'{systemtype}','sharestory',lastloadDateTable.lower())
          writefilepath=file_path+year_+ "/" + month_ + "/" + day_
          loglist.append(commonConfigs.AddLog("Processing started for table " +table.strip('"'),0))
          stageDf=getSourceData(dbs_name,selectAllQuery)
          coalesceString= getCoalesceString()
          if lastloadDate != 'None' and lastloadDate is not None:
            filteredQuery=getQryFilteredOnWatermark(filterQueryTable,lastloadDate)
            stageDf=getSourceData(dbs_name,filteredQuery)
          else:
            stageDf=stageDf
          stageCount=stageDf.count()
          loglist.append(commonConfigs.AddLog(f"Data Load Count From Postgres - {stageCount}",stageCount))
          if stageCount>0:
            try:
              readingSourceDataAndWritingToFilepath()
            except Exception as e:
              loglist.append(commonConfigs.AddLog(f"Task Failed while executing for Table: "+table.strip('"') +", with ERROR: "+ str(e)[:500],0))
              email_body='<p>The Task under <b>import_dcx_postgresdata</b> job, is failing while processing Data Import for Table: '+table.strip('"') + '<BR/><BR/><b>Exception</b>: ' + str(e)[:500] + '</p>'
              dbutils.notebook.run('/deployments/housekeeping/EmailTemplate',0,{'ebody':f'{email_body}','ename':'import_dcx_postgresdata Job Failure Notification', "job_outputs":"{}"})
          else:
            loglist.append(commonConfigs.AddLog("No Records to load for " +table.strip('"'),0))
          loglist.append(commonConfigs.AddLog("Processing completed for table " +table.strip('"'),0))
  InsertLogs(str(loglist))
except Exception as e:
  out['result'] = 'ERROR: ' + str(e)
  InsertLogs(str(loglist))
  raise e

# COMMAND ----------

out['result']='SUCCESS'
dbutils.notebook.exit(out)
