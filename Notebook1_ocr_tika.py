# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook which runs final ETL steps and 'Weglakken' associated notebook (for use with the Self-service to PDF tool)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Params

# COMMAND ----------

params = {'blobResourceName': '<resource name>', # Name of Resource in Azure
          'blob_storage_name': '<storage name>',  # Container name in Blob storage
          'wob_project_name': '<storage name>', # ProjectName to use in WOB backend and ...
          'batch_name': 'batchnametest',  # BatchName to use in WOB backend
          'version': 'development',
          'process_dir': '/dbfs/mnt/<storage name>',  # Location to mount the files and to store intermediate results (starts with /dbfs/mnt/*)
          'database_wob_table_name': '<storage name>',  # Name of table to store (Hive) meta data of files in
          'emails': 'True',
          'zipcodes': 'True',
          'street_names': 'True',
#          'Postbus' : 'True',
          'money_use': 'True',
          'money_exotic_currencies': 'False',
          'person_names1': 'True',
          'person_names2': 'True',
          'person_names3': 'True',
          'person_names4': 'True',
          'person_names5': 'True',
          'banking_acconts' : 'True',
          'telephone-numbers' : 'True',
          'client-relation': 'True',
          'BSN_number': 'True'    
         }
use_yml_file = False

# Clear all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

import json

# Init set all widgets
for key, value in params.items():
  dbutils.widgets.text(key, value)
    
if use_yml_file == True:
  # Read in the settings of the blob
  with open("/dbfs/FileStore/tables/wob_setting.yml", 'r') as stream:
      setting_wob = yaml.safe_load(stream)
  
  # Override widgets
  for key, value in params.items():
    dbutils.widgets.text(key, setting_wob.get(value, ''))

def getParams():
  global params
  return {key: dbutils.widgets.get(key) for key, value in params.items()}

print(getParams())

# COMMAND ----------

# MAGIC %md 
# MAGIC # Mount storage
# MAGIC Check if storage is already mounted. If not perform a mount.  
# MAGIC Storage account key is stored in Databricks secrets keyvault.   
# MAGIC Each blobResourceName must be added to the Databricks secrets, using this command line tool  
# MAGIC `databricks secrets put --scope WOB --key <blobResourceName>`

# COMMAND ----------

# Mount the blobstorage if not mounted yet, error message is normal if the directory is already mounted.
mount_point = dbutils.widgets.get('process_dir').replace('/dbfs','dbfs:') + '/'
if not mount_point in [i.path for i in dbutils.fs.ls('/mnt/')]:
  dbutils.fs.mount(
    source = "wasbs://" + dbutils.widgets.get('blob_storage_name') + "@" + dbutils.widgets.get('blobResourceName') + ".blob.core.windows.net",
    mount_point = mount_point.replace('dbfs:',''),
    extra_configs = {"fs.azure.account.key." + dbutils.widgets.get('blobResourceName') + ".blob.core.windows.net": dbutils.secrets.get(scope = "WOB", key = dbutils.widgets.get('blobResourceName'))}
  )
  print(f'Directory {mount_point} mounted!')
else:
  print(f'Directory {mount_point} already mounted')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if wob-resources is mounted. wob-resources contains all the adobe acrobat templates and files needed for the regex statements

# COMMAND ----------

if not 'dbfs:/mnt/wob-resources/' in [i.path for i in dbutils.fs.ls('/mnt/')]:
  print({"fs.azure.account.key." + dbutils.widgets.get('blobResourceName') + ".blob.core.windows.net": dbutils.secrets.get(scope = "WOB", key = dbutils.widgets.get('blobResourceName'))})
  dbutils.fs.mount(
    source = "wasbs://wob-resources@" + dbutils.widgets.get('blobResourceName') + ".blob.core.windows.net",
    mount_point = '/mnt/wob-resources',
    extra_configs = {"fs.azure.account.key." + dbutils.widgets.get('blobResourceName') + ".blob.core.windows.net":dbutils.secrets.get(scope = "WOB", key = dbutils.widgets.get('blobResourceName'))}
  )
  print(f'Directory /mnt/wob-resources mounted!')
else:
  print(f'Directory /mnt/wob-resources already mounted')


# COMMAND ----------

# DBTITLE 1,Dependencies
import os, subprocess, datetime
from pdfminer.pdfpage import PDFPage
from tika import parser
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get non-searcheable PDF's
# MAGIC Identify PDF's that have non-searchable pages, identify the pages and add them to list for OCR

# COMMAND ----------

def get_pdf_non_searchable_pages(fname):
  """
  Check if pdf's have non-searchable pages.
  
  @type fname: str
  @param fname: the file name
  @rtype: np.array(int)
  @return: the pages which are not searchable (with no text)
  """

    non_searchable_pages = []
    page_num = 0
    
    with open(fname, 'rb') as infile:
        for page in PDFPage.get_pages(infile):
            page_num += 1
            if not 'Font' in page.resources.keys():
              non_searchable_pages.append(page_num)
    
    return non_searchable_pages

# COMMAND ----------

# look for pdf files to process
proc_dir = dbutils.widgets.get("process_dir") + '<folder name>'
print(proc_dir)
pdf_files = []

for root, dirs, files in os.walk(proc_dir):
    for name in files:
        name_lower = name.lower()
        if name_lower.endswith(".pdf"):
          pdf_files.append(os.path.join(root, name))

print("Found " + str(len(pdf_files)), "pdf's to process")

# COMMAND ----------

# find searchable and non-searchable pdf's and add them to list
searchable=[]
non_searchable=[]
for file in pdf_files:
  if len(get_pdf_non_searchable_pages(file)) == 0:
    searchable.append(file)
  else:
    non_searchable.append(file)

print(f'searchable: {len(searchable)}')
print(f'non-searchable: {len(non_searchable)}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### OCR non-searchable pages

# COMMAND ----------

 def ocr_pdf_file(path, max_trials=3):
  """
     Method for mapping the myocr function to a Spark rdd.
     
     @type path: str
     @param path: the path to the pdf file that has to be ocr'ed
     @type max_trials: int
     @param max_trials: the maximum number of times to perform ocr
     @rtype: str, int
     @return: the file path, the error message, the number of trials needed and the non-searchable pages
  """
  
  path_out = path
  message = 'Success'
  pages = str(get_pdf_non_searchable_pages(path))[1:-1]
  print(pages)
  try:
      subprocess.check_output(['ocrmypdf', path, path_out, '--pages', pages,'--force-ocr','-l','nld','--output-type', 'pdf','--clean', '--tesseract-oem', '1']) 
      
      if os.stat(path_out).st_mtime  <  datetime.datetime.now().timestamp()-86400:
         raise Exception('File has not been written')
      
      message = 'Success'
      
  except Exception as e: 
        message = 'Failed'
        print(e)
        # Recursive step, if the maximum number of trials is greater than 0, try to convert it to pdf again. Reduce max_trials by 1 after each trial
        if max_trials > 0:
            return ocr_pdf_file(path, max_trials-1)
        else:
          return(path, "Error failed to convert:"+str(e),  max_trials)
  
  return (path, message, max_trials, 'non-searchable pages:'+ pages)

# COMMAND ----------

#Create spark dataframe for files with pages to ocr
df_non_searchable = spark.createDataFrame(non_searchable, StringType())
non_searchable_rdd = df_non_searchable.rdd
non_searchable_rdd.take(5)

# COMMAND ----------

# perform ocr for pages that are not searchable
ocr_rdd = non_searchable_rdd.map(lambda x:ocr_pdf_file(x[0])).collect()

# COMMAND ----------

# add files to searchable list for parsing
failed=[]
for file in ocr_rdd:
  if file[1]=='Success':
    searchable.append(file[0])
  else:
    failed(file[0])
print(f'searchable: {len(searchable)}')
print(f'failed ocr: {len(failed)} : {failed}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse with Tika
# MAGIC Get raw .txt out of files

# COMMAND ----------

def find_regex_matches(reg, string, group = 0, ignorecase = True):
    """
      Find a regular expression in a string. 

      @type reg: str
      @param reg: a regular expression
      @type string: str
      @param string: the text to apply the regex to
      @type group: int
      @param group: sets the regex group to mark
      @type ignorecase: bool
      @param ignorecase: whether or not to ignore case in matches, default on True
      @rtype: np.array(str)
      @return: the matched values from the regex
    """
  
    matches = []
    i=0
    while(i<len(string)):  
        if ignorecase == True:
          match = re.search(reg, string[i:len(string)], flags=re.IGNORECASE)
        else:
          match = re.search(reg, string[i:len(string)])
          
        if not match:
            return matches
        elif match:
            matches.append(match.group(group))
            i = i + match.span()[1]
    return matches
  
def check_special_characters(x, threshold=0.65):
  """
    Check for non latin characters in the text.
    If the percentage of non latin characters is too high, it is assumed that the ocr did not go well. Threshold is set at 0.65.

    @type x: str
    @param x: the file name
    @type threshold: int
    @param threshold: the threshold value for checking if ocr went well or not
    @rtype: bool
    @return: if True then the threshold is passed and ocr did not go well
  """
  quality = False
  regex_special_character = r'[^ \.a-zA-Z0-9]'
  if len(find_regex_matches(regex_special_character, x)) / len(x) > threshold:
    quality = True
  
  return quality

def check_unicode(s):
  """
    Check if a string can be converted to unicode.
   
    @type s: str 
    @param s: the file name
    @rtype: bool
    @return: if True it is assumed that the ocr has to be redone
  """
  unicode_check = False

  if isinstance(s, str):
    unicode_check = False
  elif isinstance(s, unicode):
    unicode_check = True
  else:
    print("not a string")

  return unicode_check

# COMMAND ----------

#TODO: check if file exists
def tika_parse(x, proc_dir, max_trials=3):
    """
    Convert the content of the file to txt.
   
    @type s: str
    @param s: the file name
    @type proc_dir: str
    @param proc_dir: the working directory
    @type max_trials: int
    @param max_trials: the maximum number of times to try to parse the text
    @rtype: bool
    @return: if True it is assumed that the parsing has to be redone
  """

    path =  x.replace('dbfs:','/dbfs')
    path_out = proc_dir + '/tika_parsed/'
    filename = path_out + os.path.splitext(os.path.basename(path))[0] +'.txt'
    content =''
    
    try:
         
      if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            print(exc)
               
      content = parser.from_file(path)['content']

      if content is not None: 
          with open(filename, "w") as f:
            f.write(content)
            f.close()
          
          if os.stat(filename).st_mtime  <  datetime.datetime.now().timestamp()-86400:
            raise Exception('File has not been written')
          
          message = 'Success'
                       
      elif content == None:
        return(filename, path, 'No content','No content', max_trials)
      elif check_special_characters(content) == True:
        return(filename, path, 'No content','No content', max_trials)
      elif check_unicode(content) == True:
        return(filename, path, 'No content','No content', max_trials)
                
    except Exception as e:
        message = 'Failed'
               
        # Recursive step, if the maximum number of trials is greater than 0, try to convert it to pdf again. Reduce max_trials by 1 after each trial
        if max_trials > 0:
            tika_parse(x, max_trials-1)
        else:
            return (filename, path, 'No content' ,'No content' , max_trials)    
          
    return (filename, path, message, max_trials)

# COMMAND ----------

#Create spark df for files to extract tekst
df_searchable = spark.createDataFrame(searchable, StringType())
searchable_rdd = df_searchable.rdd
searchable_rdd.take(5)

# COMMAND ----------

tika_files = searchable_rdd.map(lambda x: tika_parse(x[0], proc_dir)).collect()

# COMMAND ----------

no_content=[]
parsed=[]
for file in tika_files:
  if file[2]=='No content':
    no_content.append(file[1])
  else:
    parsed.append(file[1])
print(f'not parsed files: {len(no_content)}')
print(f'parsed files: {len(parsed)}')

# COMMAND ----------

# check the directory
for root, dirs, files in os.walk(proc_dir + 'tika_parsed/'):
    print(len(files))

# COMMAND ----------

tika_files

# COMMAND ----------

# DBTITLE 1,If some files are not parsed
# ocr does work good if performed directly on each file
for file in searchable:
  print(file)
  ocr_pdf_file(path=file)
#   tika_parse(file, proc_dir)

# COMMAND ----------

no_content=[]
for file in tika_files:
  if file[2]=='No content':
    no_content.append(file[1])
print(f'not parsed files: {len(no_content)}')

# COMMAND ----------

# check the directory
for root, dirs, files in os.walk(proc_dir + '/tika_parsed'):
    print(len(files))

# COMMAND ----------

# MAGIC %md
# MAGIC # Weglakken

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weglak detection en redact file

# COMMAND ----------

result = dbutils.notebook.run("/Shared/WOB/WOB_template_selfservicetool/Weglakken/Weglakken_selfservicetool", 6000, getParams()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store project and batch in Status-Backend

# COMMAND ----------

# save data to be used later for PowerBI rapport
# result = dbutils.notebook.run("/Shared/WOB/WOB_workflow_template/2.Weglakken/2.2 API", 6000, getParams())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount container

# COMMAND ----------

#dbutils.fs.unmount(dbutils.widgets.get('process_dir').replace('/dbfs',''))

# COMMAND ----------

print("Finished!")

# COMMAND ----------


