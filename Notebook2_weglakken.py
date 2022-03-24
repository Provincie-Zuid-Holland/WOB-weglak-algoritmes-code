# Databricks notebook source
# USE THIS FOR STANDONLY RUNNING.

# initial set params
local_run = True

if local_run == True:
  params = {'blobResourceName': '<resource name>',  # Name of Resource in Azure
            'blob_storage_name': '<storage name>',  # Container name in Blob storage
            'wob_project_name': '<storage name>', # ProjectName to use in WOB backend and in the adobe installation files 
            'batch_name': 'batchnametest',  # BatchName to use in WOB backend
            'version': 'development',
            'process_dir': '/dbfs/mnt/<storage name>',  # Location to mount the files and to store intermediate results (starts with /dbfs/mnt/*)
            'database_wob_table_name': '<storage name>',  # Name of table to store (Hive) meta data of files in
            'emails': 'True',
            'zipcodes': 'True',
            'street_names': 'True',
#             'Postbus' : 'True',
            'money_use': 'True',
            'money_exotic_currencies': 'False',
            'person_names1': 'True',
            'person_names2': 'True',
            'person_names3': 'True',
            'person_names4': 'True',
            'person_names5': 'True',
            'persoon_opvat': 'True',
            'banking_acconts' : 'True',
            'telephone-numbers' : 'True',
            'client-relation': 'True',
            'BSN_number': 'True'    
           }

# # Clear all widgets
dbutils.widgets.removeAll()


# COMMAND ----------

# Init set all widgets
for key, value in params.items():
  dbutils.widgets.text(key, value) 

# COMMAND ----------

project_name = dbutils.widgets.get("database_wob_table_name") ## Batch name
input_wob_location = dbutils.widgets.get("process_dir").replace('/dbfs','') + '<Path to folder>/tika_parsed' ## Container location 

# Output bestanden/
output_location = dbutils.widgets.get("process_dir") + '/Output_ProDC'
output_bestanden = output_location + "/bestanden"

dict_description = "Lijst met gedetecteerde persoonsgegevens"

# COMMAND ----------

print(input_wob_location)

# COMMAND ----------

import os, re, subprocess
import pandas as pd
import numpy as np
from jinja2 import Template

# COMMAND ----------

# make folder for output bestanden
if not os.path.exists(output_location):
  subprocess.check_output(['mkdir', output_location, '-p'])
if not os.path.exists(output_bestanden):
  subprocess.check_output(['mkdir', output_bestanden, '-p'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up overall functions

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

# COMMAND ----------

class Trie():
  """
    Trie is a tree-like data structure made up of nodes. There are two major operations that can be performed on a trie, namely:
      - Inserting a word into the trie
      - Searching for words using a prefix
    The trie can be exported to a regex pattern.
    The corresponding regex should match much faster than a simple Regex union.
  """  
  def __init__(self):

    self.data = {}

  def add(self, word):

    ref = self.data
    for char in word:
        ref[char] = char in ref and ref[char] or {}
        ref = ref[char]
    ref[''] = 1

  def dump(self):

    return self.data

  def quote(self, char):
    return re.escape(char)

  def _pattern(self, pData):
    data = pData
    if "" in data and len(data.keys()) == 1:
        return None

    alt = []
    cc = []
    q = 0
    for char in sorted(data.keys()):
        if isinstance(data[char], dict):
            try:
                recurse = self._pattern(data[char])
                alt.append(self.quote(char) + recurse)
            except:
                cc.append(self.quote(char))
        else:
            q = 1
    cconly = not len(alt) > 0

    if len(cc) > 0:
        if len(cc) == 1:
            alt.append(cc[0])
        else:
            alt.append('[' + ''.join(cc) + ']')

    if len(alt) == 1:
        result = alt[0]
    else:
        result = "(?:" + "|".join(alt) + ")"

    if q:
        if cconly:
            result += "?"
        else:
            result = "(?:%s)?" % result
    return result

  def pattern(self):
    return self._pattern(self.dump())
  
def trie_regex_from_words(words):
    """
      Make a trie regex expression from a python array of strings.
      
      @type words: np.array(str)
      @param words: a python array of strings to make a regex trie from
      @rtype: str
      @return: a compiled regex statement with a regex trie
    """
    trie = Trie()
    for word in words:
        trie.add(word[0])
    return r"\b" + trie.pattern() + r"\b"

def lidwoord_lower_case(last_name):
  """
    Belgium last names have a uppercase articles.
    Dutch last names don't so this method puts them to lowercase.
    
    @type last_name: str
    @param last_name: the surname
    @rtype: str
    @return: the surname in lower case
  """
  
  try:  
    split_word = last_name.split(' ')

    if len(split_word) == 2:
        split_word[0] =  split_word[0].lower()
        return (split_word[0]+" "+split_word[1])
    elif len(split_word) == 3:
        split_word[0] =  split_word[0].lower()
        split_word[1] =  split_word[1].lower()
        return(split_word[0]+" "+split_word[1]+" "+split_word[2])
    else:
        return last_name
      
  except:
    print(last_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Overall variables

# COMMAND ----------

# New data set has higher recall, contains first names for ~100,000 Dutch surnames along with Belgian surnames (10k) and names from Azure AD.
first_names = pd.read_csv('<Path to file with first names>', encoding="utf-8-sig", sep=',')

# Last names for ~200,000 Dutch surnames along with Belgian surnames (10k) and surnames from Azure AD.
last_names = pd.read_csv('<Path to file with surnames>', encoding="utf-8-sig", sep=',')

# Street adresses
straatnamen = pd.read_csv('<Path to file with street names>', encoding="utf-8-sig", sep='\t')

#make regex expressions from the csv files and set them into global variables.
regex_addresses_statement = trie_regex_from_words(straatnamen.values)

# The single regex statement to look for first names and last names.
regex_names_statement1 = trie_regex_from_words(first_names.values)+" "+trie_regex_from_words(last_names.values)
regex_names_statement2 = "(mevrouw |dhr[.]? |mr[.]? |mw[.]? |Mw[.] Mr[.] |mevr[.]? |Mevr[.]? |dr[.]? |drs[.]? |meneer |de heren |De heren |de heer |De heer |Mevrouw |Dhr[.]? |Mr[.]? |Mw[.]? |Dr[.]? |Drs[.]? |Prof[.]? |prof[.]? |Meneer |de Heren| Mts[.]? |ir[.]? )(([A-Z][.]? ?(en)? ?[A-Z]?[.]? ?){1,4})?"+trie_regex_from_words(last_names.values)
regex_names_statement3 = "([A-Z][.]? ?(en)? ?[A-Z]?[.]? ?){1,4}"+trie_regex_from_words(last_names.values)
regex_names_statement4 = trie_regex_from_words(last_names.values)+ "(( )|(, ?))([A-Z][.]? ?(en)? ?[A-Z]?[.]? ?){1,4}"
regex_names_statement5 = "(hi ?|Hi ?|hoi ?|Hoi ?|hallo ?|Hallo ?|hey ?|Hey ?|beste ?|Beste ?|Dag ?|Groet,? ?!?|groet,? ?|groetjes,? ?|Groetjes,? ?|gr,? ?|Gr,? ?|Met vriendelijke groet(en)?,? ?|met vriendelijke groet(en)?,? ?|Mvg,? ?|mvg,? ?|Hartelijke groet,? ?|hartelijke groet,? ?|Vr[.] ?gr[.],? ?|vr[.] ?gr[.],? ?|Alvast bedankt,? ?|Alvast bedankt en hartelijke groet,? ?)"+trie_regex_from_words(first_names.values)

# Persoonlijke opvatting
persoonlijk_opvatting = pd.read_csv('<Path to file with personal comments>', encoding="utf-8-sig", header=None) #geeft in indicatie waar persoonlijk opvattingen kunnen zijn
voornaamwoorden = pd.read_csv('<Path to file with pronouns>', encoding="utf-8-sig", header=None)
regex_persoon_opvat = trie_regex_from_words(voornaamwoorden.values) +" "+ trie_regex_from_words(persoonlijk_opvatting.values)

# COMMAND ----------

# Enable filters.
param_personal_details = {
    'emails': dbutils.widgets.get('emails'), 
    'zipcodes': dbutils.widgets.get('zipcodes'), 
    'street_names':dbutils.widgets.get('street_names'), 
#     'Postbus': dbutils.widgets.get('Postbus'),
    'money_use': dbutils.widgets.get('money_use'),
    'money_exotic_currencies': dbutils.widgets.get('money_exotic_currencies'),
    'person_names1' : dbutils.widgets.get('person_names1'),
    'person_names2' : dbutils.widgets.get('person_names2'),
    'person_names3' : dbutils.widgets.get('person_names3'),
    'person_names4' : dbutils.widgets.get('person_names4'),
    'person_names5' : dbutils.widgets.get('person_names5'),
    'persoon_opvat' : dbutils.widgets.get('persoon_opvat'),
    'banking_acconts' : dbutils.widgets.get('banking_acconts'),
    'telephone-numbers' :  dbutils.widgets.get('telephone-numbers'),
    'client-relation' : dbutils.widgets.get('client-relation'),
    'BSN_number': dbutils.widgets.get('BSN_number')
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Regular expression functions

# COMMAND ----------

def find_emails(document):
    """
        Extract email addresses.
        
        @type document: str
        @param document: a document to be searched for emails
        @rtype: str
        @return: the found regex matches with emails
    """ 
    pattern_email = r'[a-zA-Z0-9_.+-]+@(?=[@a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
    matches = list(set(find_regex_matches(pattern_email, document, ignorecase = True)))    
    return  matches
  
def find_money(safe_text, exotic_currencies= False):
  """
      Combines several regex statements to find money in text together.

      @type safe_text: str
      @param safe_text: a particular text to match on
      @type exotic_currencies: str
      @param exotic_currencies: besides euro also other currencies are identified
      @rtype: list(str)
      @return: all the found regulair expressions
  """
  #look for numbers without symbol xxx,00 xxx.00?
  
  geld =''
  # Find matches with start with a currency sign and then have a digit behind them and extra statements
  if exotic_currencies == 'True':
    find_currency_before =  r'(\\xe2\\x82\\xac|\€|$|\\xc2\\xa3|£|\\xe2\\x82\\xba|₺|eurocent|euro|eur|gulden|nlg|cent|usd|gbp|chf|swiss franc|Zwitserse frank|zl|pln|turkse lira|penny|pence|czk|kr|levs|dollar)( )?\d+?((\,\d+)+)?((\.\d+)+)?( mln| miljoen| miljard| honderd| duizend)?\b( en )?(\d+((\,\d+)+)?((\.\d+)+)?(\,\-)?)?( mln| miljoen| miljard| honderd| duizend )?'
    # Find matches which start with a digit, optional dutch bedrag amounts and ends with a currency.
    find_currency_after = r'(\d+?((\,\d+)+)?((\.\d+)+)?( miljard | miljoen | mln | honderd | duizend )?( )?(\$ |£ |₺ |eurocent|euro|eur|gulden|nlg|cent|usd|gbp|chf|swiss franc|Zwitserse frank|zl|pln|turkse lira|penny|pence|czk|kr|levs|dollar|€ )\b)'
    # Find text excluding money text value's followed by a currency sign.
    text_currency_find = r'\b([A-z]+)\b(?<![mln|miljoen|miljard]) \b(\\xe2\\x82\\xac|€|\$|\\xc2\\xa3|£|\\xe2\\x82\\xba|₺|eurocent|euro|eur|gulden|nlg|cent|usd|gbp|chf|swiss franc|Zwitserse frank|zl|pln|turkse lira|penny|pence|czk|kr|levs|dollar)\b'
  
  else:
    #old code does not get ,-- and ,00 in 1.200,00
    find_currency_before =  r'(eurocent|euro|eur|€|\$)()?( )?(  )?(   )?(    )?(     )?(\d+((\,\d+)+)?((\.\d+)+)?((\,\d+)+)?)( mln| miljoen| miljard| honderd| duizend| million| billion)?\b( en )?(\d+((\,\d+)+)?((\.\d+)+)?((\,\d+)+)?([\,\-]+)?)?( mln| miljoen| miljard| honderd| duizend| million| billion )?\b'
#     find_currency_before =  r'(eurocent|euro|eur|€|\$)()?( )?(  )?(   )?(    )?(     )?\d+?((\,\d+)+)?((\.\d+)+)?( mln| miljoen| miljard| honderd| duizend| million| billion)?\b( en )?(\d+((\,\d+)+)?((\.\d+)+)?(\,\-)?)?( mln| miljoen| miljard| honderd| duizend| million| billion )?'
    
    # Find matches which start with a digit, optional dutch bedrag amounts and ends with a currency.
    find_currency_after = r'(\d+?((\,\d+)+)?((\.\d+)+)?((\,\d+)+)?()?( )?(   )?(    )?(     )?( miljard | miljoen | mln | honderd | duizend | million | billion )?( )?(€|\$|eurocent|euro|eur|cent|cents)\b)'
#     find_currency_after = r'(\d+((\,\d+)+)?((\.\d+)+)?()?( )?(   )?(    )?(     )?( miljard | miljoen | mln | honderd | duizend | million | billion |)?( )?(eurocent|euro|eur|cent|cents|€|\$))'
    
    # Find text excluding money text value's followed by a currency sign.
    text_currency_find = r'\b([A-z]+)\b(?<![mln|miljoen|miljard|million|billion]) \b(eurocent|euro|eur|cent|cents)\b'
  
    geld = find_regex_matches(find_currency_before, safe_text, ignorecase = True)
    geld = geld + find_regex_matches(find_currency_after, safe_text, ignorecase = True)
    geld = geld + find_regex_matches(text_currency_find, safe_text, ignorecase = True)
  
  return list(set(geld))

def find_banking_accounts(safe_text):
    """    
      Container method to evaluate different regex expersions for kvk,iban,old dutch banking accounts, btw and vestigingsnumber.
      Uses capturing groups to return only the matched digits and not the entire string, e.g. return 123456 when detecting KvK 123456

      @type: str
      @param safe_text: the text to evaluate
      @rtype: list(str)
      @return: the found matches
    """   
    rkvk = r'(k\.?v\.?k[\.:]?|chamber\sof\scommerce[:]?|handelsregister[:]?).{0,15} ?(\d{8})' 
    riban = r'NL\s?(\d[\s\.]?){2}([A-Z][\s\.]?){4}(\d[\s\.]?){10}\b|BE\s?(\d[\s\.]?){2}(\d[\s\.]?){12}\b|DE\s?(\d[\s\.]?){2}(\d[\s\.]?){18}\b|CH\s?(\d[\s\.]?){2}(\d[\s\.]?){17}\b'
    rrkn = r'((bank)?rek(ening)?(nummer)?(\.)?( )?(nr)?(\,)?(:)?|bank|Bankgironr)([ \.:]{0,20} ?)(\d{2}\.\d{2}.\d{2}.\d{3})'  
    rbtw = r'(btw)?(\-)?(nr(\.)? |nummer )?NL([ -])?\d+((\,\d+)+)?((\.\d+)+)?(\.)?B\d{2}'
    rvst = r'(vestiging(snummer)?).{0,15} (\d{12})'
    rdossier = r'(dossier)( ?nummer|nr)?.{0,15} ?((\d{6})([A-Za-z]{1})?)'
    rbrs = r'(BRS\-? ?nummer[\s\.\-: ]{0,15}) ?(\d{9})\b'
    
    bankingaccounts = find_regex_matches(rkvk, safe_text, 2, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(riban,safe_text, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(rrkn,safe_text, 11, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(rbtw,safe_text, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(rvst,safe_text, 3, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(rdossier,safe_text, 3, ignorecase = True)
    bankingaccounts = bankingaccounts+find_regex_matches(rbrs,safe_text, 2, ignorecase = True)
    
    return list(set(bankingaccounts))
  
def find_postalcode(document):
    """
      Regex expression for finding postal codes.
      
      @type document: str
      @param document: the text to check
      @rtype: list(str)
      @return: the found matches of the regex expression
    """
    pattern_postcode = r'\b[1-9]{1}\d{3}( |)?[A-Z]{2}\b'
    postal_code = list(set(find_regex_matches(pattern_postcode, document, ignorecase=False)))
    return postal_code
  
def find_street(document,regex_addresses_statement):
    """
      Regex expression for finding street names.
      
      @type document: str
      @param document: the text examine
      @type regex_addresses_statement: str
      @param regex_addresses_statement: Trie regex for street names
      @rtype: list(str)
      @return: the found matches of the regex expression.
    """
    street = list(set(find_regex_matches((regex_addresses_statement + ' ' + r'[0-9]+[A-Z]?\b'), document, ignorecase=False)))
    return street
  
# def find_postbus(document):
#     """
#       Regex expression for finding postbus.

#       @type document: str
#       @param document: the text examine
#       @rtype: list      
#       @return: the found matches of the regex expression
#     """
#     pattern_postbus = r'(Postbus |P.O. Box |B.P. )[0-9]+'
#     postbus = list(set(find_regex_matches(pattern_postbus, document, ignorecase=True)))
#     return postbus
  
def find_names(safe_text, regex_statement):
   """
        Combines several regex statements to find names in text together.
        At the moment the recall is high though the precision.

        @type safe_text: str
        @param safe_text: a particular text to match on
        @param regex_statement: regex trie statement for names
        @rtype: list(str)
        @return: the found regex expressions
   """
   
   names = find_regex_matches(regex_statement, safe_text, ignorecase = False)
   
   return list(set(names))
  
def find_comment(safe_text, regex_statement):
   """
        Combines 2 lists to find personnal comments.
        
        @type safe_text: str
        @param safe_text: a particular text to match on
        @type regex_statement: str
        @param regex_statement: regex trie statement for personal comments
        @rtype: list(str)
        @return: the found regex expressions.
   """
   
   comment = find_regex_matches(regex_statement, safe_text, ignorecase = True)
   
   return list(set(comment))
  
def find_phonenrs(safe_text): 
    """
      Extract phone numbers.

      @type safe_text: str
      @param safe_text: document to be scanned for phone numbers
      @rtype: list(str)
      @return: the found matches
    """
    
    rphone1 = r'([+] ?31|0031|06[-. ]{1,3})([\d]{4})([ -][\d]{4})'
    rphone2 = r'([+] ?31|[+] ?316|0031|06[-. ]{1,3})([ ]?[\d]{1,4})([ ][\d]{1,4})([ ][\d]{1,4})([ ][\d]{1,4})?([ ][\d]{1,4})?'
    rphone3 = r'([+] ?31|[+] ?316|0031|06[-. ]{0,3})([ ]?[\d]{8,9})'
    rphone4 = r'(T(el)?(efoon)?|Phone|Mobiel|F(ax)?|Doorkiesnr)([-.: ]{0,2})(0[\d\- ]{9,13})' 
    
    phonenrs = find_regex_matches(rphone1, safe_text, ignorecase = True)
    phonenrs = phonenrs+find_regex_matches(rphone2, safe_text, ignorecase = True)
    phonenrs = phonenrs+find_regex_matches(rphone3, safe_text, ignorecase = True)
    phonenrs = phonenrs+find_regex_matches(rphone4, safe_text, 6, ignorecase = True)
    
    return list(set(phonenrs))
  
def find_client_relation(safe_text):
    """
      Find the client relation number.

      @type safe_text: str
      @param safe_text: the text to evaluate
      @rtype: list(str)
      @return: the found matches.
    """
    
    rclient = r'(Klant|Zaak ?(nummer|nr|nr\.)?[ :*] ?)([\d\-\:\. ]{3,15}\b)'
    rrelation = r'(Rel\.?(atie)? ?(nummer|nr|nr\.)?[ :*] ?)([\d\-\:\.]{3,15}\b)'
    rFF = r'FF (([\d\-\:\. ]{3,5})([\w\. ]{3,15})([\d\. ]{3,5}))\b'
    
    clientrel = find_regex_matches(rclient,safe_text, 3, ignorecase=True)
    clientrel = clientrel+find_regex_matches(rrelation,safe_text, 4, ignorecase=True)
    clientrel = clientrel+find_regex_matches(rFF,safe_text, 1, ignorecase=True)
    
    return list(set(clientrel))
  
def elfproef(bsn):
  """
    Check if de number is valid. All bsn's consist of nine digits and must be devidable by 11.

    @type bsn: str
    @param bsn: the number to check
    @rtype: bool
    @return: if True then the number qualifies as bsn
  """
  som = 0
  
  try:
    if len(bsn) == 9:
      for i in range(len(bsn) - 1):
        som = som + int(bsn[i])*(9-i)
      som = som + int(bsn[-1])*-1
      
  except Exception as e:
    print(e)
    
  isElfproef = np.select([(som % 11 == 0) and (som != 0), som == 0], [True, False], default=False)
  
  return isElfproef
  
def find_bsn(document):
    """
      Extract Dutch burger service number, nine digits i.e. 231298811. This number has to be 'Elfproef', see https://nl.wikipedia.org/wiki/Elfproef
      
      @type document: str
      @param document: text to scan
      @rtype: list(str)
      @return: found bsn number
    """
    pattern = r'((bsn|burgerservicenummer|Burgerservice nummer)[ :*] ?)(\b[0-9]{9}\b)'
    bsn = find_regex_matches(pattern, document, 3, ignorecase=True)

    if elfproef(bsn):
      bsn = bsn
    
    return list(set(bsn))

# COMMAND ----------

# MAGIC %md
# MAGIC # Binding function

# COMMAND ----------

def search_file(file, safe_text, regex_names_statement1, regex_names_statement2, regex_names_statement3, regex_names_statement4, regex_names_statement5, regex_persoon_opvat, regex_addresses_statement, output_location, param_personal_details, write_file_toblob = False ):
    """    
      This method combines the finding of sensitive information and thus the main entry point.
      
      @type file: str
      @param file: the file name
      @type safe_text: str
      @param safe_text: the content of the file
      @type regex_names_statement1: str
      @param regex_names_statement1: first regex statement to look for first names
      @type regex_names_statement2: str
      @param regex_names_statement2: first regex statement to look for surnames
      @type regex_names_statement3: str
      @param regex_names_statement3: second regex statement to look for surnames
      @type regex_names_statement4: str
      @param regex_names_statement4: second regex statement to look for surnames
      @type regex_names_statement5: str
      @param regex_names_statement5: first regex statement to look for first names
      @type regex_persoon_opvat: str
      @param regex_persoon_opvat: statement to look for personal comments
      @type regex_addresses_statement: str
      @param regex_addresses_statement: statement to look for addresses
      @type output_location: str
      @param output_location: path to the folder with the results
      @type param_personal_details: bool
      @param param_personal_details: sets up de filters
      @type write_file_toblob: bool
      @param write_file_toblob: if set to True, the list of found recognized entities is saved as .csv in the output_location
      @rtype: pd.DataFrame
      @return: the file name and the recognized entities
    """
    df_all = pd.DataFrame()
        
    safe_text = safe_text.decode('utf-8', errors = "ignore")
    
    if param_personal_details['emails'] == 'True':
      # Emails.
       df_all = df_all.append([pd.DataFrame({'match':find_emails(safe_text), 'match_type':'emails'})], ignore_index=True)
    
    if param_personal_details['zipcodes'] == 'True':
      # Postcodes.
      df_all = df_all.append([pd.DataFrame({'match':find_postalcode(safe_text), 'match_type':'Postcode'})],ignore_index=True)
    
    if param_personal_details['street_names'] == 'True':
      # Straatnaam.
      df_all = df_all.append([pd.DataFrame({"match": find_street(safe_text, regex_addresses_statement), 'match_type':'Straat'})],ignore_index=True)
    
#     if param_personal_details['Postbus'] == 'True':
#       # Postbus.
#       df_all = df_all.append([pd.DataFrame({"match": find_postbus(safe_text), 'match_type':'Postbus'})],ignore_index=True)
    
    if param_personal_details['money_use'] == 'True':
      # Geld.
      df_all = df_all.append([pd.DataFrame({"match": find_money(safe_text,param_personal_details['money_exotic_currencies']), 'match_type':'Geldbedrag'})],ignore_index=True) 
      
    if param_personal_details['person_names1'] == 'True':
      # Voornaam + achternaam
      df_all = df_all.append([pd.DataFrame({"match": find_names(safe_text, regex_names_statement1), 'match_type':'Persoonsnamen1'})],ignore_index=True)
    
    if param_personal_details['person_names2'] == 'True':
      # Aanhef(+afkorting) + achternaam
      df_all = df_all.append([pd.DataFrame({"match": find_names(safe_text, regex_names_statement2), 'match_type':'Persoonsnamen2'})],ignore_index=True)
      
    if param_personal_details['person_names3'] == 'True':
      # Afkorting + achternaam
      df_all = df_all.append([pd.DataFrame({"match": find_names(safe_text, regex_names_statement3), 'match_type':'Persoonsnamen3'})],ignore_index=True)
    
    if param_personal_details['person_names4'] == 'True':
      # Achternaam + afkorting
      df_all = df_all.append([pd.DataFrame({"match": find_names(safe_text, regex_names_statement4), 'match_type':'Persoonsnamen4'})],ignore_index=True)  
      
    if param_personal_details['person_names5'] == 'True':
      #Aanhef + voornaam
      df_all = df_all.append([pd.DataFrame({"match": find_names(safe_text, regex_names_statement5), 'match_type':'Persoonsnamen5'})],ignore_index=True)
      
    if param_personal_details['persoon_opvat'] == 'True':
      #persoonlijk opvatting
      df_all = df_all.append([pd.DataFrame({"match": find_comment(safe_text, regex_persoon_opvat), 'match_type':'PersoonlijkOpvatting'})],ignore_index=True)
      
    if param_personal_details['banking_acconts'] == 'True':
      # Bankgegevens.
      df_all = df_all.append([pd.DataFrame({"match": find_banking_accounts(safe_text), 'match_type':'Bankgegevens'})],ignore_index=True)
    
    if param_personal_details['telephone-numbers'] == 'True':
      # Telefoonnummers.
      df_all = df_all.append([pd.DataFrame({"match": find_phonenrs(safe_text), 'match_type':'Telefoonnummers'})],ignore_index=True)
    
    if param_personal_details['client-relation'] == 'True':
      # Klant/Relatie.
      df_all = df_all.append([pd.DataFrame({"match": find_client_relation(safe_text), 'match_type':'Klant/Relatie'})],ignore_index=True)
    
    if param_personal_details['BSN_number'] == 'True':
      # BSN nummer.
      bsn_results = find_bsn(safe_text)
      if len(bsn_results) > 0:
        df_all = df_all.append([({"match":bsn_results, 'match_type':'BSN nummer'})],ignore_index=True)

    #df_all.drop_duplicates(inplace=True)
   
    getfilename = file.split('/')
    getfilename = getfilename[(len(getfilename)-1)]
    df_all['filename'] = getfilename
    if write_file_toblob == True:
      df_all.to_csv(output_location)
  
    return(df_all)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up the RDD

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
# MAGIC sc.hadoopConfiguration.get("mapreduce.input.fileinputformat.input.dir.recursive")

# COMMAND ----------

# For reading the parsed tika files
rdd = sc.binaryFiles(input_wob_location).cache()

# COMMAND ----------

# rdd.take(5)

# COMMAND ----------

# only for debug purposes
#sc.binaryFiles(input_wob_location).cache().take(5)

# COMMAND ----------

# Run the regex on the rdd
results = rdd.map(lambda x:search_file(x[0].replace('dbfs:/', '/dbfs/'), x[1], regex_names_statement1, regex_names_statement2, regex_names_statement3, regex_names_statement4, regex_names_statement5, regex_persoon_opvat, regex_addresses_statement,output_location,param_personal_details)).collect()

# COMMAND ----------

len(results)

# COMMAND ----------

results[2]

# COMMAND ----------

# make dictionary with results
mydict = pd.concat(results)
mydict.loc[mydict['match_type'] !='Geldbedrag', "exemptioncode"] = "art10.2e"
mydict.loc[mydict['match_type'] =='Geldbedrag', "exemptioncode"] = "art10.2b"
mydict.loc[mydict['match_type'] =='PersoonlijkOpvatting', "exemptioncode"] = "art11"
mydict = mydict[['match','exemptioncode','match_type','filename']]
mydict.columns = ['entity','exemptioncode','type','filename']
mydict

# COMMAND ----------

mydict = mydict.astype(str)
mydict.drop_duplicates(subset='entity', inplace=True)
mydict

# COMMAND ----------

# Remove items which should never be redacted
no_redact_path = '<Path to file with items not to be redacted>'
no_redact_file = pd.read_csv(no_redact_path, encoding="utf-8-sig", sep='\t')

gedeputeerden_ZH = '/dbfs/mnt/wob-resources/dictionary/gedeputeerden_ZH.csv' #do not mark province deputies
gedeputeerden = pd.read_csv(gedeputeerden_ZH, encoding="utf-8-sig", sep='\t', header=None)

contact_gemeenten = '/dbfs/mnt/wob-resources/dictionary/Gemeenten_gegevens.csv'
gemeenten = pd.read_csv(contact_gemeenten,encoding="utf-8-sig", sep=';')
gemeenten = gemeenten['adres'].append(gemeenten['Telefoon']).append(gemeenten['email2']).append(gemeenten['adres2']).append(gemeenten['adres3'])
# gemeenten

mydict_corr = mydict.drop(mydict[mydict.entity.isin(no_redact_file['entity'])|mydict.entity.isin(gedeputeerden)|mydict.entity.isin(gemeenten)].index.tolist())
mydict_corr.reset_index(inplace = True)
mydict_corr.drop(['index'], axis=1, inplace=True)
mydict_corr

# COMMAND ----------

# Save dictionary to csv (to be checked by the wob team)
outfile_dic = dbutils.widgets.get("process_dir") + '/dictionary.csv'
mydict_corr.to_csv(outfile_dic, index=False, encoding="utf-8-sig", sep ='\t')

# COMMAND ----------

# After getting feedback from the wob team, read the corrected dictionary
df_path = dbutils.widgets.get("process_dir") + '/dict_corr.csv'
dict_corr = pd.read_csv(df_path, encoding="utf-8-sig", sep=';')
dict_corr                     

# COMMAND ----------

# # Add items that should be redacted
# extra_redact_path = dbutils.widgets.get("process_dir") + '/extra_redact.csv'
# extra_redact_file = pd.read_csv(extra_redact_path, encoding="utf-8-sig", sep=';')
# dict_corr = dict_corr.append(extra_redact_file,ignore_index=True)
# dict_corr

# COMMAND ----------

# Convert dictionary to autoredact format
## Create dictionary .cfs file using autoredacts formatting
dict_list = ["<</Dict0 [/c <<	/Desc [/t ({dict_description})]".format(dict_description=dict_description)]
dict_list.append("	/Entries [/c <<		")
## Insert all found entities to redact
for row in dict_corr.iterrows():    
    dict_list.append("		/{index} [/c <<			/code2 [/t ({exemption_code})]".format(index = row[1].name, exemption_code = row[1]['exemptioncode']))
    dict_list.append("			/word2 [/t ({keyword})]".format(keyword = row[1]['entity']))
    dict_list.append(">>]")
dict_list.append(">>]")
dict_list.append("	/ExcludeWords [/c <<		/NumItems [/i 0]")
dict_list.append(">>]")
dict_list.append("	/Name [/t ({dict_name})]".format(dict_name = project_name))
dict_list.append(">>]")
dict_list.append("/NumGroups [/i 1]")
dict_list.append(">>")

# COMMAND ----------

# DBTITLE 1,Write Adobe acrobat dictionary
# Write the autoredact dictionary to databricks filestore
auto_redact_file = os.path.join(output_bestanden, project_name) + ".cfs"

outF = open(auto_redact_file, "w")
for line in dict_list:
  # write line to output file
  outF.write(line)
  outF.write("\n")
outF.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose Adobe version (Pro DC or X pro) and only markup or autoredact

# COMMAND ----------

# DBTITLE 1,Write wizard and .bat file for Acrobat Pro DC for Autobatch markup
# Write the Adobe action wizard file to Databricks filestorage
action_wizard_file = os.path.join(output_bestanden, project_name) + ".sequ"
with open('<Path to template_markup_DC.sequ.jinja2>') as file_: #use this file to mark.
    template = Template(file_.read())
template.stream(project_name = project_name,
               dictionary_name = project_name).dump(action_wizard_file)

# Make .bat file to install the dictionary and wizard file
install_name = os.path.join(output_location,"bestanden_installeren") + ".bat"

# Write the installation file to Databricks filestorage
with open('<Path to bestanden_installeren_template.bat>') as file_:
    template = Template(file_.read())
template.stream(wob_project=project_name).dump(install_name)

# Make .bat file for AutoBatch
autobatch_name = os.path.join(output_location,"AutoBatch") + ".bat"

# Write the Autobatch file to Databricks filestorage
with open('<Path to template_AutoBatch_markup.bat>') as file_:
    template = Template(file_.read())
template.stream(wob_project=project_name).dump(autobatch_name)

# COMMAND ----------

# DBTITLE 1,Write wizard and .bat file for Acrobat Pro DC (for complete autoredact)
# # Write the Adobe action wizard file to Databricks filestorage
# action_wizard_file = os.path.join(output_bestanden, project_name) + ".sequ"
# with open('/dbfs/mnt/wob-resources/adobe/wob_template.sequ.jinja2') as file_:
#     template = Template(file_.read())
# template.stream(wob_project=project_name,
#                dictionary_name = project_name).dump(action_wizard_file)

# # Make .bat file to install the dictionary and wizard file
# install_name = os.path.join(output_location,"bestanden_installeren") + ".bat"

# # Write the installation file to Databricks filestorage
# with open('/dbfs/mnt/wob-resources/adobe/bestanden_installeren_template.bat') as file_:
#     template = Template(file_.read())
# template.stream(wob_project=project_name).dump(install_name)

# COMMAND ----------

# DBTITLE 1,Write wizard and .bat file for Acrobat X Pro (for complete autoredact)
# # Write the Adobe action wizard file to Databricks filestorage
# action_wizard_file = os.path.join(output_bestanden, project_name) + ".sequ"
# with open('/dbfs/mnt/wob-resources/adobe/template_XPro.sequ.jinja2') as file_:
#     template = Template(file_.read())
# template.stream(wob_project=project_name,
#                dictionary_name = project_name).dump(action_wizard_file)

# # Make .bat file to install the dictionary and wizard file
# install_name = os.path.join(output_location,"bestanden_installeren_XPro") + ".bat"

# # Write the installation file to Databricks filestorage
# with open('/dbfs/mnt/wob-resources/adobe/bestanden_installeren_template_XPro.bat') as file_:
#     template = Template(file_.read())
# template.stream(wob_project=project_name).dump(install_name)

# # Copy autoredact configuration files to folder 'bestanden'
# import shutil
# copy_autoredact = shutil.copy('/dbfs/mnt/wob-resources/adobe/configuration_files/AutoRedact.cfs', output_bestanden)
# copy_wob_codes = shutil.copy('/dbfs/mnt/wob-resources/adobe/configuration_files/AutoRedactCodesLocal.cfs', output_bestanden)

# COMMAND ----------


