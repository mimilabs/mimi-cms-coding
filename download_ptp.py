# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.cms.gov"
page = "/medicare/coding-billing/national-correct-coding-initiative-ncci-edits/medicare-ncci-procedure-procedure-ptp-edits"
volumepath = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"

# COMMAND ----------

response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')
files_to_download = [f'{url_base}{str(Path(a["href"]))}'.replace('/license/ama?file=', '')
                    for a in soup.find_all('a', href=True)
                    if (a['href'].endswith('.zip'))]
filenames = [f'ptp_{file.split("/")[-1]}'
             for file in files_to_download]
download_files(files_to_download, volumepath, filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip

# COMMAND ----------

volumepath_zip = "/Volumes/mimi_ws_1/cmscoding/src/zipfiles"
volumepath_unzip = "/Volumes/mimi_ws_1/cmscoding/src/ptp"

# COMMAND ----------

for file in Path(volumepath_zip).glob('*'):
    if (re.match('ptp.+', file.name)):
        folder = file.stem.split('-')[2]
        unzip(file, volumepath_unzip + '/' + folder)

# COMMAND ----------


