# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## ICD10-CM

# COMMAND ----------

files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.icd10cm").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/icd10cm/"
for file in Path(volumepath).rglob("icd10cm_codes*"):
    
    if "_addenda_" in file.name:
        continue
    if file.name in files_exist:
        continue
    
    mimi_src_file_name = file.name
    mimi_src_file_date = parse(f"{file.stem[-4:]}-09-30").date() # ICD10 lifecycle, Oct 1 ~ Sept 30
    mimi_dlt_load_date = datetime.today().date()
    data = []
    with open(file, "r") as f:
        for line in f.readlines():
            row = [line[:8].strip(), line[8:].strip(), 
                   mimi_src_file_date, 
                   mimi_src_file_name,
                   mimi_dlt_load_date]
            data.append(row)
    pdf = pd.DataFrame(data, columns=["code", 
                                "description", 
                                "mimi_src_file_date", 
                                "mimi_src_file_name", 
                                "mimi_dlt_load_date"])
    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable(f"mimi_ws_1.cmscoding.icd10cm"))       

# COMMAND ----------

# MAGIC %md
# MAGIC ## POA Exempt

# COMMAND ----------

files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.icd10cm_poa_exempt").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/icd10cm_poa/"
for file in Path(volumepath).rglob("POAexemptCodes*"):
    if ".txt" not in file.name:
        continue
    if file.name in files_exist:
        continue
    
    mimi_src_file_name = file.name
    mimi_src_file_date = parse(f"20{file.stem[-2:]}-09-30").date() # ICD10 lifecycle, Oct 1 ~ Sept 30
    mimi_dlt_load_date = datetime.today().date()
    data = []
    with open(file, "r") as f:
        for line in f.readlines()[1:]:
            row = [line.split('\t')[1], 
                   line.split('\t')[2], 
                   mimi_src_file_date, 
                   mimi_src_file_name,
                   mimi_dlt_load_date]
            data.append(row)
    pdf = pd.DataFrame(data, columns=["code", 
                                "description", 
                                "mimi_src_file_date", 
                                "mimi_src_file_name", 
                                "mimi_dlt_load_date"])
    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable(f"mimi_ws_1.cmscoding.icd10cm_poa_exempt"))       

# COMMAND ----------

# MAGIC %md
# MAGIC ## ICD10-PCS

# COMMAND ----------

files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.icd10pcs").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/icd10pcs/"
for file in Path(volumepath).rglob("icd10pcs_codes*"):
    
    if file.name in files_exist:
        continue
    
    mimi_src_file_name = file.name
    mimi_src_file_date = parse(f"{file.stem[-4:]}-09-30").date() # ICD10 lifecycle, Oct 1 ~ Sept 30
    mimi_dlt_load_date = datetime.today().date()
    data = []
    with open(file, "r") as f:
        for line in f.readlines():
            row = [line[:8].strip(), line[8:].strip(), 
                   mimi_src_file_date, 
                   mimi_src_file_name,
                   mimi_dlt_load_date]
            data.append(row)
    pdf = pd.DataFrame(data, columns=["code", 
                                "description", 
                                "mimi_src_file_date", 
                                "mimi_src_file_name", 
                                "mimi_dlt_load_date"])
    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable(f"mimi_ws_1.cmscoding.icd10pcs"))       

# COMMAND ----------

# MAGIC %md
# MAGIC ## HCPCS

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.cmscoding.hcpcs

# COMMAND ----------

files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.hcpcs").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/hcpcs/"
for file in Path(volumepath).rglob("HCPC*.xls*"):
    if ("_changes" in file.name.lower() or 
        "_corrections" in file.name.lower() or
        "_trans" in file.name.lower()):
        continue
    if file.name in files_exist:
        continue
    
    month_map = {"JAN": "01-01", "APR": "04-01", "JUL": "07-01", "OCT": "10-01"}
    skiprows_map = {"HCPC2020_APRIL_ANWEB_w_disclaimer_v5.xlsx": 10,
                    "HCPC2021_APR_CONTR_ANWEB.xlsx": 9,
                    "HCPC2020_OCT_ANWEB.xlsx": 10
                    }
    mimi_src_file_date = parse(file.name[4:8] + "-" + month_map[file.name[9:12]]).date()
    mimi_src_file_name = file.name
    mimi_dlt_load_date = datetime.today().date()
    data = []
    
    pdf = pd.read_excel(file, dtype=str, skiprows=skiprows_map.get(file.name, 0))
    pdf["ASC_DT"] = pd.to_datetime(pdf["ASC_DT"]).dt.date
    pdf["ADD DT"] = pd.to_datetime(pdf["ADD DT"]).dt.date
    pdf["ACT EFF DT"] = pd.to_datetime(pdf["ACT EFF DT"]).dt.date
    pdf["TERM DT"] = pd.to_datetime(pdf["TERM DT"]).dt.date
    
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = mimi_dlt_load_date

    pdf = pdf.drop(columns=['price3', 'price4', 'cim3',
                      'labcert5', 'labcert6', 'labcert7', 'labcert8',
                      'xref3', 'xref4', 'xref5',
                      'opps', 'opps_pi', 'opps_dt', 'tos5'])

    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.cmscoding.hcpcs"))      
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## NCCI

# COMMAND ----------

files_exist = {}

# COMMAND ----------

files_exist = {x[0] for x in spark.read.table("mimi_ws_1.cmscoding.ncci_mue").select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/cmscoding/src/ncci/"
for file in Path(volumepath).rglob("*.xlsx"):
    if file.name in files_exist:
        continue
    tokens = file.stem.split('_')
    service_type = tokens[2] 
    mimi_src_file_date = parse(tokens[4][:10]).date()
    mimi_src_file_name = file.name
    mimi_dlt_load_date = datetime.today().date()
    pdf = pd.read_excel(file, dtype=str, skiprows=1)

    pdf.columns = change_header(pdf.columns)
    pdf = pdf.rename(columns={
            'hcpcscpt_code': 'hcpcs_cpt_code',
            'practitioner_services_mue_values': 'mue_values',
            'outpatient_hospital_services_mue_values': 'mue_values',
            'dme_supplier_services_mue_values': 'mue_values'})
    
    pdf['mue_values'] = pd.to_numeric(pdf['mue_values'], errors='coerce')
    pdf["service_type"] = service_type
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = mimi_dlt_load_date
    
    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"mimi_ws_1.cmscoding.ncci_mue"))      
    

# COMMAND ----------


