# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## ICD10

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.cms.gov"
page = "/medicare/coding-billing/icd-10-codes"
volumepath = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"

# COMMAND ----------

response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')
pages = []
for a in soup.find_all('a', href=True):
    if re.match('\d+ ICD-10.+', a.text) is not None:
        pages.append(a['href'])

# COMMAND ----------

pages = [x for x in pages if x.split('/')[-1].split('-')[0] > '2024']

# COMMAND ----------

for page in sorted(pages, reverse=True):
    prefix = page.split('/')[-1]
    response = requests.get(f"{url_base}{page}")
    response.raise_for_status()  # This will raise an error if the fetch fails
    soup = BeautifulSoup(response.text, 'html.parser')
    files_to_download = [f'{url_base}{str(Path(a["href"]))}'
                        for a in soup.find_all('a', href=True)
                        if (a['href'].endswith('.zip'))]
    filenames = []
    for file in files_to_download:
        if '/files/zip/' in file:
            filenames.append(f'{prefix}_{file.split("/files/zip/")[-1].replace("/","-")}')
        elif '/icd10/downloads/' in file: 
            filenames.append(f'{prefix}_{file.split("/icd10/downloads")[-1].replace("/","-")}')

    download_files(files_to_download, volumepath, filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HCPCS

# COMMAND ----------

page = "/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update"

# COMMAND ----------

response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')
files_to_download = [f'{url_base}{str(Path(a["href"]))}'
                    for a in soup.find_all('a', href=True)
                    if (a['href'].endswith('.zip'))]
filenames = [f'hcpcs_{file.split("/")[-1]}' for file in files_to_download]
download_files(files_to_download, volumepath, filenames)

# COMMAND ----------

files_to_download = [f'{url_base}{str(Path(a["href"]))}'
                    for a in soup.find_all('a', href=True)
                    if (a['href'].endswith('.zip-0'))]
filenames = [f'hcpcs_{file.split("/")[-1]}' for file in files_to_download]
download_files(files_to_download, volumepath, filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NCCI MUE (Medically Unlikely Edit)

# COMMAND ----------

page = "/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-mue-archive"

# COMMAND ----------

response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')
files_to_download = [f'{url_base}{str(Path(a["href"]))}'
                    for a in soup.find_all('a', href=True)
                    if (a['href'].endswith('.zip'))]
filenames = [f'ncci_{file.split("/")[-1]}' for file in files_to_download]
download_files(files_to_download, volumepath, filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/icd10cm"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('\d+-icd-10-cm.+descriptions.+', file.name) or
        (re.match('\d+-icd-10-cm.+codes.zip', file.name) and '-poa-' not in file.name)):
        unzip(file, volumepath_unzip)

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/icd10pcs"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('\d+-icd-10-pcs.+codes.+', file.name)):
        unzip(file, volumepath_unzip)

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/hcpcs"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('hcpcs.+', file.name)):
        unzip(file, volumepath_unzip)

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/ncci"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('ncci.+', file.name)):
        unzip(file, volumepath_unzip)

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/icd10cm_poa"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('\d+-icd-10-cm.+poa-exempt', file.name)):
        unzip(file, volumepath_unzip)

# COMMAND ----------


