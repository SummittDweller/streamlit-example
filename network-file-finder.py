# network-file-finder.py
##
## This script is designed to read all of filenames from a specified --column of a specified
## --worksheet Google Sheet and fuzzy match with files found in a specified --tree-path 
## network storage directory tree.
##
## If the --copy-to-azure option is set this script will attempt to deposit copies of any/all
## OBJ files it finds into Azure Blob Storage.  If --extended (-x) is also specified, the script will also 
## search for and copy all _TN. and _JPG. files (substituting those for _OBJ.) that it finds.  
## The copy-to-azure operation will also generate a .csv file containing Azure Blob URL(s) suitable 
## for input into the `object_location`, `image_small`, and `image_thumb` columns of a CollectionBuilder CSV ## file or Google Sheet.

import sys
import getopt
import re
import gspread as gs
import csv
import os.path
from fuzzywuzzy import fuzz, process
import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Local packages
import my_colorama

# Globals
azure_base_url = "https://dgobjects.blob.core.windows.net/"
column = 7     # Default column for filenames is 'G' = 7 
skip_rows = 1  # Default number of header rows to skip = 1
levehstein_ratio = 90
significant = False
kept_file_list = False
copy_to_azure = False
extended = False
grinnell = False
use_match_list = False
counter = 0
csvlines = [ ]
big_file_list = [ ]   # need a list of just filenames...
big_path_list = [ ]   # ...and parallel list of just the paths
significant_file_list = [ ]
significant_path_list = [ ] 
significant_dict = { }


# --- Function definitions


# BIG_function( ) - The old processing guts of this script made into a function
# --------------------------------------------------------------------------------------
def BIG_function(kept_file_list, path, counter):

  csvlines = [ ]

  # Check the --kept-file-list switch.  If it is True then attempt to open the file-list.tmp file 
  # saved from a previous run.  The intent is to cut-down on Google API calls.
  if kept_file_list:
    try:
      with open('file-list.tmp', 'r') as file_list:
        for filename in file_list:
          if filename:
            filenames.append(filename.strip( ))
          else: 
            filenames.append("")

    except Exception as e:
      kept_file_list = False
      pass  

  # If we don't have a kept file list... Open the Google service account and sheet
  else:
    try:
      sa = gs.service_account()
    except Exception as e:
      my_colorama.red(e)
    
    try:
      sh = sa.open_by_url(sheet)
    except Exception as e:
      my_colorama.red(e)
  
    gid = int(extract_sheet_id_from_url(sheet))
    worksheets = sh.worksheets()
    worksheet = [w for w in sh.worksheets() if w.id == gid]
    
    # Grab all filenames from --column 
    filenames = worksheet[0].col_values(column)  
    try:
      with open('file-list.tmp', 'w') as file_list:
        for filename in filenames:
          file_list.write(f"{filename}\n")
    except Exception as e:
      my_colorama.red("Unable to open temporary file 'file-list.tmp' for writing.")
      exit( )

    # Grab all non-hidden filenames from the target directory tree so we only have to get the list once
    # Exclusion of dot files per https://stackoverflow.com/questions/13454164/os-walk-without-hidden-folders

    for root, dirs, files in os.walk(path):
      files = [f for f in files if not f[0] == '.']
      dirs[:] = [d for d in dirs if not d[0] == '.']
      for filename in files:
        big_path_list.append(root)
        big_file_list.append(filename)

    # Check for ZERO network files in the big_file_list
    if len(big_file_list) == 0:
      my_colorama.red(f"The specified --tree-path of '{path}' returned NO files!  Check your path specification and network connection!\n")
      exit( )

    # Report our --regex option...
    if significant:
      my_colorama.green(f"\nProcessing only files matching signifcant --regex of '{significant}'!")
    else:
      my_colorama.green(f"\nNo --regex specified, matching will consider ALL paths and files.")

    # Now the main matching loop...
    for x in range(len(filenames)):
      if x < skip_rows:  # skip this row if instructed to do so 
        my_colorama.yellow(f"Skipping match for '{filenames[x]}' in worksheet row {x}")
        continue         # move on and process the next row
      
      counter += 1
      target = filenames[x]
      
      # If --grinnell is specified and the 'target' begins with 'grinnell_' AND does not contain '_OBJ'... make it so
      if grinnell and ('grinnell_' in target) and ('_OBJ' not in target):
        target += '_OBJ.'

      my_colorama.green(f"\n{counter}. Finding best fuzzy filename matches for '{target}'...")
      csv_line = [ ]  
      significant_text = ''

      (significant_text, significant_file_list, significant_path_list, significant_dict) = build_lists_and_dict(significant, target, big_file_list, big_path_list)    

      report = "None"
      if significant_text:
        my_colorama.blue(f"  Significant string is: '{significant_text}'.")
        report = significant_text
      
      # If target is blank, skip the search and set matches = False
      matches = False
      if len(target) > 0:
        matches = process.extract(target, significant_dict, limit=3)
      
      # Append new line to CSV regardless if there was a match or not
      csv_line.append(f"{counter}")
      csv_line.append(target)
      csv_line.append(report)

      # Report the top three matches
      if matches:
        for found, (match, score, index) in enumerate(matches):
          path = significant_path_list[index]
          csv_line.append(f"{score}")
          csv_line.append(match)
          csv_line.append(path)
          if found==0: 
            # txt = ' | '.join(csv_line)
            my_colorama.green("!!! Found BEST matching file: {}".format(csv_line))

      else:
        csv_line.append('0')
        csv_line.append('NO match')
        csv_line.append('NO match')
        my_colorama.red("*** Found NO match for: {}".format(' | '.join(csv_line)))

      # Save this fuzzy search result in 'csvlines' for return
      csvlines.append(csv_line)

    # If --output-csv is true, open a .csv file to receive the matching filenames and add a heading
    if output_to_csv:
      with open('match-list.csv', 'w', newline='') as csvfile:
        listwriter = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

        if significant:
          significant_header = f"'{significant}' Match"
        else:  
          significant_header = "Undefined"

        header = ['No.', 'Target', 'Significant --regex', 'Best Match Score', 'Best Match', 'Best Match Path', '2nd Match Score', '2nd Match', '2nd Match Path', '3rd Match Score', '3rd Match', '3rd Match Path']
        listwriter.writerow(header)

        for line in csvlines:
          listwriter.writerow(line)

    return csvlines


# read_match_list_csv( )
# --------------------------------------------------------------------------------------------------
def read_match_list_csv( ):
  csvlines = [ ]
  try:
    with open('match-list.csv', 'r') as csvfile:
      reader_obj = csv.reader(csvfile)
      for index, row in enumerate(reader_obj): 
          if index > 0:
            csvlines.append(row)
  except Exception as e:
    my_colorama.red("Exception: ")
    my_colorama.red(f"{e}")
    exit

  return csvlines


# upload_to_azure( ) - Just what the name says post-processing
# ----------------------------------------------------------------------------------------------
def upload_to_azure(blob_service_client, target, score, match, upload_file_path):
  try:
    
    # Check if the match score was 90 or above, if not, don't copy it!
    if score < 90:
      msg = f"Best match for '{target}' has an insufficient match score of {score}.  It will NOT be copied to Azure storage."
      my_colorama.red(msg)
      return False

    # Determine which container ['objs','thumbs','smalls'] for this file
    container_name = False

    if "_TN." in match:
      container_name = 'thumbs'   
      url = azure_base_url + "thumbs/" + match 
    elif "_JPG." in match:
      container_name = 'smalls'   
      url = azure_base_url + "smalls/" + match 
    elif "_OBJ." in match:
      container_name = 'objs'
      url = azure_base_url + "objs/" + match 
    else:
      container_name = 'objs'
      url = azure_base_url + "objs/" + match 


    # Create a blob client using the local file name as the name for the blob
    if container_name:
      blob_client = blob_service_client.get_blob_client(container=container_name, blob=match)
      if blob_client.exists( ):
        msg = f"Blob '{match}' already exists in Azure Storage container '{container_name}'.  Skipping this upload."
        my_colorama.yellow(msg)
      else:  
        msg = f"Uploading '{match}' to Azure Storage container '{container_name}'"
        my_colorama.blue(msg)
        # Upload the file
        with open(file=upload_file_path, mode="rb") as data:
          blob_client.upload_blob(data)
      
    else:  
      msg = f"No container available for uploading '{match}' to Azure Storage!'"
      my_colorama.red(msg)
      return False

    return url

  except Exception as ex:
    my_colorama.yellow('Exception:')
    my_colorama.yellow(f"{ex}")
    pass


# extract_sheet_id_from_url(url)
# ---------------------------------------------------------------------------------------
def extract_sheet_id_from_url(url):
  res = re.compile(r'#gid=([0-9]+)').search(url)
  if res:
    return res.group(1)
  raise Exception('No valid sheet ID found in the specified Google Sheet.')

# excel_column_number(name)     
# from https://stackoverflow.com/questions/7261936/convert-an-excel-or-spreadsheet-column-letter-to-its-number-in-pythonic-fashion
# ------------------------------------------------------------------------------
def excel_column_number(name):
  """Excel-style column name to number, e.g., A = 1, Z = 26, AA = 27, AAA = 703."""
  n = 0
  for c in name:
    n = n * 26 + 1 + ord(c) - ord('A')
  return n

# check_significant(regex, filename)
# ---------------------------------------------------------------------------------------
def check_significant(regex, filename):
  import re

  if '(' in regex:             # regex already has a (group), do not add one
    pattern = regex
  else: 
    pattern = f"({regex})"     # regex is raw, add a (group) pair of parenthesis

  try:
    match = re.search(pattern, filename) 
    if match:
      return match.group( )
    else:
      return False
  except Exception as e:
    assert False, f"Exception: {e}"  


# build_lists_and_dict(significant, target, files_list, paths_list)
# ---------------------------------------------------------------------------------------
def build_lists_and_dict(significant, target, files_list, paths_list):
  significant_file_list = []
  significant_path_list = [] 
  significant_match = False
  is_significant = "*"

  # If a --regex (significant) was specified see if our target has a matching component...
  if significant:
    significant_match = check_significant(significant, target)
    if significant_match:   # ...it does, pare the significant_*_list down to only significant matches
      for i, f in enumerate(files_list): 
        is_significant = check_significant(significant_match, f)
        if is_significant:
          significant_file_list.append(f)
          significant_path_list.append(paths_list[i])
  
  # If there's no significant_match... make the output lists match the input lists
  if not significant_match:
    significant_file_list = files_list
    significant_path_list = paths_list 

  # Now, per https://github.com/seatgeek/fuzzywuzzy/issues/165 build an indexed dict of significant files
  file_dict = {idx: el for idx, el in enumerate(significant_file_list)}

  # Return a tuple of significant match and the three significant lists
  return (significant_match, significant_file_list, significant_path_list, file_dict)    


# --- Main

if __name__ == '__main__':

  # --- Arg handling
  # arg handling per https://www.tutorialspoint.com/python/python_command_line_arguments.htm
  
  my_colorama.blue(f"\nNumber of arguments: {len(sys.argv[1:])}")
  my_colorama.blue(f"Argument List:' {str(sys.argv[1:])}\n")

  args = sys.argv[1:]
  output_to_csv = False

  try:
    opts, args = getopt.getopt(args, 'haokmxgw:c:t:r:s:', ["help", "copy-to-azure", "output-csv", "kept-file-list", "extended", "grinnell", "use-match-list", "worksheet=", "column=", "tree-path=", "regex=", "skip-rows="])
  except getopt.GetoptError:
    my_colorama.yellow("python3 network-file-finder.py --help --copy-to-azure --output-csv --kept-file-list --extended --grinnell --use-match-list --worksheet <worksheet URL> --column <worksheet filename column> --tree-path <network tree path> --regex <significant regex> \n")
    sys.exit(2)

  # Process the command line arguments
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      my_colorama.yellow("python3 network-file-finder.py --help --output-csv --kept-file-list --worksheet <worksheet URL> --column <filename column> --tree-path <network tree path> --regex <significant regex> --skip-rows <number of header rows to skip> --copy-to-azure --extended --grinnell --use-match-list\n")
      sys.exit( )
    elif opt in ("-w", "--worksheet"):
      sheet = arg
    elif opt in ("-c", "--column"):
      if arg.isalpha() and arg.isupper():
        column = excel_column_number(arg)
      else:
        my_colorama.red("Unhandled option: Column must be an uppercase character or string using only letters A through Z.")
        exit( )
    elif opt in ("-t", "--tree-path"):
      path = arg
    elif opt in ("-r", "--regex"):
      significant = arg
    elif opt in ("-s", "--skip-rows"):
      try:
        val = int(arg)
        if val >= 0:
          skip_rows = val
        else:
          my_colorama.red("Unhandled option: Number of rows to skip must be an integer >= 0.")
          exit( )
      except ValueError:
        my_colorama.red("Unhandled option: Number of rows to skip must be an integer >= 0")
        exit( )
    elif opt in ("-o", "--output-csv"):
      output_to_csv = True
    elif opt in ("-a", "--copy-to-azure"):
      copy_to_azure = True
    elif opt in ("-k", "--kept-file-list"):
      kept_file_list = True
    elif opt in ("-m", "--use-match-list"):
      use_match_list = True
    elif opt in ("-x", "--extended"):
      extended = True
    elif opt in ("-g", "--grinnell"):
      grinnell = True
    else:
      my_colorama.red("Unhandled command line option")
      exit( )

    # elif opt in ("-f", "--fuzzy-score"):
    #   try:
    #     val = int(arg)
    #     if val >= 0 and val <= 100:
    #       levehstein_ratio = val
    #       break
    #     else:
    #       assert False, "Unhandled option: Fuzzy score must be an integer between 0 and 100."
    #   except ValueError:
    #     assert False, "Unhandled option: Fuzzy score must be an integer between 0 and 100."

  # Create an empty list of filenames    
  filenames = [ ]

  # If --use-match-list then open previous fuzzy search results from `match-list.csv` and skip to post-processing
  if use_match_list:
    csvlines = read_match_list_csv( )

  # Not using the match-list.csv results... call the BIG function!
  else:
    csvlines = BIG_function(kept_file_list, path, counter)    

## Post-processing...
## ------------------------------------------------------------------------------------------

# If --copy-to-azure is true... for each '_OBJ.' (and if --extended '_TN.' or '_JPG.') match 
# execute a copy to Azure Blob Storage operation.  For this to work our AZURE_STORAGE_CONNECTION_STRING
# environment variable must be in place and accurate.  
#
if copy_to_azure:
  msg = f"\n\tBeginning copy_to_azure process for {len(csvlines)} objects.\n\t"
  my_colorama.blue(msg)

  try:

    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
    # created after the application is launched in a console or with Visual Studio,
    # the shell or application needs to be closed and reloaded to take the
    # environment variable into account.

    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Open a CSV file to accept `object_location`, `image_small`, and 
    # `image_thumb` columns of Azure URLs.
    urls_for_csv = open("object_urls.csv", "w")
    csv_handler = csv.writer(urls_for_csv)

    # Loop on all the "matches"
    for line in csvlines:
      # print(line)
      index = int(line[0])
      target = line[1]
      score = int(line[3])
      match = line[4]
      path = line[5]

      # Build a network file path for the best match
      upload_file_path = os.path.join(path, match)

      # Do it and return an Azure Blob URL for the object
      url = upload_to_azure(blob_service_client, target, score, match, upload_file_path)

      tn_url = False
      jpg_url = False
      urls = ["", "", ""]

      # If --extended is on... try again for a _TN. file and _JPG. file
      if extended:
        
        tn = target.replace("_OBJ.", "_TN.jpg")
        upload_file_path = os.path.join(path, tn)
        if os.path.isfile(upload_file_path):
          tn_url = upload_to_azure(blob_service_client, tn, 100, tn, upload_file_path)

        jpg = target.replace("_OBJ.", "_JPG.jpg")
        upload_file_path = os.path.join(path, jpg)
        if os.path.isfile(upload_file_path):
          jpg_url = upload_to_azure(blob_service_client, jpg, 100, jpg, upload_file_path)

      # Build a set of 3 Azure URLs, some may be blank, and write them to our CSV file.
      if url:
        urls[0] = url
      if jpg_url:
        urls[1] = jpg_url  
      if tn_url:
        urls[2] = tn_url  

      csv_handler.writerow(urls)


  except Exception as ex:
    my_colorama.red('Exception:')
    my_colorama.red(f"{ex}")

