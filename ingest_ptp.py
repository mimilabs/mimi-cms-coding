# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## NCCI PTP

# COMMAND ----------

files_exist = {}

# COMMAND ----------

if spark.catalog.tableExists("mimi_ws_1.cmscoding.ncci_ptp"):
    files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.ncci_ptp").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

qmap = {'q1': '-01-01', 'q2': '-03-01', 'q3': '-06-01', 'q4': '-09-01'}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/ptp/"
for file in Path(volumepath).rglob("cci*.[Tt][Xx][Tt]"):
    if file.name in files_exist:
        continue
    date_token = str(file).split('/')[6]
    mimi_src_file_date = parse(date_token[:4] + qmap[date_token[4:]]).date()
    mimi_src_file_name = file.name
    mimi_dlt_load_date = datetime.today().date()
    pdf = pd.read_csv(file, dtype=str, 
                      delimiter="\t", 
                      skiprows=6, # txt always has 6 description lines
                      header=None,
                      names=['procedure_1', 
                             'procedure_2', 
                             'in_existence_prior_to_1996',
                             'effective_date',
                             'deletion_date',
                             'modifier_allowed', 
                             'ptp_edit_rationale']
                      )
    pdf['ptp_category'] = ('practitioner' if file.stem.startswith('ccipra') 
                            else 'hospital')
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = mimi_dlt_load_date
    (
        spark.createDataFrame(pdf)
            .write
            .mode("overwrite")
            .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable('mimi_ws_1.cmscoding.ncci_ptp')
    )

# COMMAND ----------


