# streamlit-app.py
##
## This script is designed to read all of filenames from a specified --column of a specified
## --worksheet Google Sheet and fuzzy match with files found in a specified --tree-path
## network (or other mounted) storage directory tree.
##
## If the --copy-to-azure option is set this script will attempt to deposit copies of any/all
## OBJ files it finds into Azure Blob Storage.  If --extended (-x) is also specified, the script will also
## search for and copy all _TN. and _JPG. files (substituting those for _OBJ.) that it finds.
## The copy-to-azure operation will also generate a .csv file containing Azure Blob URL(s) suitable
## for input into the `object_location`, `image_small`, and `image_thumb` columns of a CollectionBuilder CSV ## file or Google Sheet.

import os
import streamlit as st
import json
import gspread as gs
import re
import csv
from fuzzywuzzy import process

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
sheet_url = False

# Functions defined and used in https://gist.github.com/benlansdell/44000c264d1b373c77497c0ea73f0ef2
# ---------------------------------------------------------------------

def update_dir(key):
    choice = st.session_state[key]
    if os.path.isdir(os.path.join(st.session_state[key+'curr_dir'], choice)):
        st.session_state[key+'curr_dir'] = os.path.normpath(os.path.join(st.session_state[key+'curr_dir'], choice))
        files = sorted(os.listdir(st.session_state[key+'curr_dir']))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files

def st_file_selector(st_placeholder, path, label='Select a file/folder', key='dir_selector_'):
    if key+'curr_dir' not in st.session_state:
        base_path = '.' if path is None or path is '' else path
        base_path = base_path if os.path.isdir(base_path) else os.path.dirname(base_path)
        base_path = '.' if base_path is None or base_path is '' else base_path

        files = sorted(os.listdir(base_path))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files
        st.session_state[key+'curr_dir'] = base_path
    else:
        base_path = st.session_state[key+'curr_dir']

    selected_file = st_placeholder.selectbox(label=label, 
                                        options=st.session_state[key+'files'], 
                                        key=key, 
                                        on_change = lambda: update_dir(key))
    
    selected_path = os.path.normpath(os.path.join(base_path, selected_file))
    st_placeholder.write(os.path.abspath(selected_path))

    if st_placeholder.button("Submit Directory Selection", "stfs_submit_button", "Click here to confirm your directory selection"):
        return selected_path
    
# My functions
# ---------------------------------------------------------------------


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


# fuzzy-search-for-files(status)
# All parameters come from st.session_state...
# --------------------------------------------------------------------------------------
def fuzzy_search_for_files(status ):

    # Get st.session_state parameters
    kept_file_list = state('use_previous_file_list')
    sheet_url = state('google_sheet_url')
    worksheet_title = state('google_worksheet_selection')
    column = state('worksheet_column_number')
    path = state('stfs_path_selection')
    regex = state('regex_text')

    csvlines = [ ]
    counter = 0
    filenames = [ ]

    # Check the --kept-file-list switch.  If it is True then attempt to open the `file-list.tmp`` file 
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

    # If we aren't using a kept file list... Open the Google service account and sheet
    else:

        try:
            sa = gs.service_account()
        except Exception as e:
            st.exception(e)
    
        try:
            sh = sa.open_by_url(sheet_url)
        except Exception as e:
            st.exception(e)

        # Open the specified worksheet (tab)
        worksheet = sh.worksheet(worksheet_title)
    
        # Grab all filenames from --column 
        filenames = worksheet.col_values(column)  
        
        # Save the filename list in 'file-list.tmp' for later
        try:
            with open('file-list.tmp', 'w') as file_list:
                for filename in filenames:
                    file_list.write(f"{filename}\n")
        except Exception as e:
            st.exception(e)
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
        st.error(f"The specified --tree-path of '{path}' returned NO files!  Check your path specification and network connection!\n")
        exit( )

    # # Report our --regex option...
    # if significant:
    #   my_colorama.green(f"\nProcessing only files matching signifcant --regex of '{significant}'!")
    # else:
    #   my_colorama.green(f"\nNo --regex specified, matching will consider ALL paths and files.")

    # Now the main matching loop...
    for x in range(len(filenames)):
        if x < skip_rows:  # skip this row if instructed to do so 
            st.warning(f"Skipping match for '{filenames[x]}' in worksheet row {x}")
            continue         # move on and process the next row
    
        counter += 1
        target = filenames[x]
    
        # # If --grinnell is specified and the 'target' begins with 'grinnell_' AND does not contain '_OBJ'... make it so
        # if grinnell and ('grinnell_' in target) and ('_OBJ' not in target):
        #     target += '_OBJ.'

        status.update(label=f"{counter}. Finding best fuzzy filename matches for '{target}'...", expanded=True, state="running")
        # st.write(f"{counter}. Finding best fuzzy filename matches for '{target}'...")
        csv_line = [ ]  
        significant_text = ''

        (significant_text, significant_file_list, significant_path_list, significant_dict) = build_lists_and_dict(significant, target, big_file_list, big_path_list)    

        report = "None"
        # if significant_text:
        #     st.status(f"  Significant string is: '{significant_text}'.")
        #     report = significant_text

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
                    st.success(f"!!! Found BEST matching file: {format(csv_line)}")

        else:
            csv_line.append('0')
            csv_line.append('NO match')
            csv_line.append('NO match')
            st.warning(f"*** Found NO match for: {format(' | '.join(csv_line))}")

        # Save this fuzzy search result in 'csvlines' for return
        csvlines.append(csv_line)

        # If --output-csv is true, open a .csv file to receive the matching filenames and add a heading
        if state('output_to_csv'):
            with open('match-list.csv', 'w', newline='') as csvfile:
                list_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

                if state('significant'):
                    significant_header = f"\'{state('significant')}\' Match"
                else:  
                    significant_header = "Undefined"

                header = ['No.', 'Target', 'Significant --regex', 'Best Match Score', 'Best Match', 'Best Match Path', '2nd Match Score', '2nd Match', '2nd Match Path', '3rd Match Score', '3rd Match', '3rd Match Path']
                list_writer.writerow(header)

                for line in csvlines:
                    list_writer.writerow(line)

    st.success(f"**Fuzzy search output is saved in 'match-list.csv**")
    status.update(label=f"Fuzzy search is **complete**!", expanded=True, state="complete")

    return csvlines







# n2a(n) - Convert spreadsheet column position (n) to a letter designation per
# https://stackoverflow.com/questions/23861680/convert-spreadsheet-number-to-column-letter
# -------------------------------------------------------------------------------
def n2a(n):
    d, m = divmod(n,26) # 26 is the number of ASCII letters
    return '' if n < 0 else n2a(d-1)+chr(m+65) # chr(65) = 'A'


# state(key) - Return the value of st.session_state[key] or False
# -------------------------------------------------------------------------------
def state(key):
    try:
        if st.session_state[key]:
            return st.session_state[key]
        else:
            return False
    except Exception as e:
        # st.exception(f"Exception: {e}")
        return False


# transform_list_to_dict(worksheet_list)
# ---------------------------------------------------------------------
def transform_list_to_dict(wks_dict, worksheet_list):
    for w in worksheet_list:
        parts = re.split('\'|:', str(w))
        wks_dict[parts[1]] = parts[3].rstrip('>')
    return wks_dict


# get_tree( )
# ---------------------------------------------------------------------
def get_tree( ):

    # Read 'paths.json' file
    with open('paths.json', 'r') as j:
        paths = json.load(j)

    # Cannot wrap this in a form because st_file_selector( ) has a callback function
    with st.container(border=True):
        selected_root = st.selectbox('Choose a mounted root directory to navigate from', paths.keys( ), index=None, key='root_directory_selectbox')   
        st.session_state.root_directory_selection = selected_root

        if state('root_directory_selection'):
            root = paths[state('root_directory_selection')]
            st.success(f"Selected root directory: **\'{state('root_directory_selection')}\' with a path of \'{root}\'**")

            st_file_selector(st, path=root, label="Select a directory root to search for the worksheet's list of files")
            st.session_state.stfs_path_selection = state('dir_selector_curr_dir')

            if state("stfs_path_selection"):
                st.success(f"Selected folder path: **\'{state('stfs_path_selection')}\'**")

    return


# get_worksheet_column_selection( )
# ----------------------------------------------------------------------        
def get_worksheet_column_selection( ):

    # Wrap all the worksheet column selection in a form...
    with st.form('worksheet_form'):

        # Read 'sheets.json' file
        with open('sheets.json', 'r') as j:
            sheets = json.load(j)


        selected_google_sheet = st.selectbox('Choose a Google Sheet to work with', sheets.keys( ), index=None, key='google_sheet_selectbox')   
        st.session_state.google_sheet_selection = selected_google_sheet

        if state("google_sheet_selection"):
            sheet_url = sheets[state("google_sheet_selection")]
            st.session_state.google_sheet_url = sheet_url
            st.success(f"Selected Google Sheet: \'{state('google_sheet_selection')}\' with a URL of \'{sheet_url}\'")

            selected_worksheet = state("google_worksheet_selection")

            # Open the Google service account 
            try:
                sa = gs.service_account( )
            except Exception as e:
                st.error(e)
    
            # Open the sheet
            try:
                sh = sa.open_by_url(sheet_url)
            except Exception as e:
                st.error(e)

            # Fetch list of worksheets and build a name:gid dict 
            worksheet_list = sh.worksheets( )
            worksheet_dict = { }
            worksheet_dict = transform_list_to_dict(worksheet_dict, worksheet_list)
    
            # Select the worksheet to be processed
            selected_worksheet = st.selectbox('Choose the worksheet you wish to work with', worksheet_dict.keys( ), index=None, key='worksheet_selectbox')   
            st.session_state.google_worksheet_selection = selected_worksheet

            if state("google_worksheet_selection"):
                st.success(f"Selected worksheet: '{selected_worksheet}' with gid={worksheet_dict[selected_worksheet]}")
                # Open the selected worksheet
                worksheet = sh.worksheet(state("google_worksheet_selection"))

                # Now fetch a list of columns from the selected sheet
                column_list = worksheet.row_values(1)

                selected_column = st.selectbox('Choose the column containing your filenames', column_list, index=None, key='column_selector')   
                st.session_state.worksheet_column_selection = selected_column

                if state('worksheet_column_selection'):
                    position = column_list.index(selected_column)
                    st.session_state['worksheet_column_number'] = position + 1   # column 'A'=1, not zero
                    col_letter = n2a(position)
                    st.success(f"Selected column: \'{state('worksheet_column_selection')}\' with designation \'{col_letter}\'") 

        st.form_submit_button("Submit Worksheet Selection")

    return 


# ----------------------------------------------------------------------        
# --- Main

if __name__ == '__main__':

    # Initialize the session_state
    if not state('root_directory_selection'):
        st.session_state.root_directory_selection = "/Users/mcfatem"
    if not state('google_sheet_selection'):
        st.session_state.google_sheet_selection = None
    if not state('google_sheet_url'):
        st.session_state.google_sheet_url = None
    if not state('google_worksheet_selection'):
        st.session_state.google_worksheet_selection = None
    if not state('worksheet_column_selection'):
        st.session_state.worksheet_column_selection = None
    if not state('worksheet_column_number'):
        st.session_state.worksheet_column_number = None
    if not state('stfs_path_selection'):
        st.session_state.stfs_path_selection = None
    if not state('use_previous_file_list'):
        st.session_state.use_previous_file_list = False
    if not state('regex_text'):
        st.session_state.regex_text = False
    if not state('output_to_csv'):
        st.session_state.output_to_csv = False

    # Display and fetch options up top
    with st.container(border=True):

        use_previous_file_list = st.checkbox(label="Check here to use the previous list of filenames stored in 'file-list.tmp'", value=False, key='use_previous_file_list_checkbox')
        st.session_state.use_previous_file_list = use_previous_file_list

        regex_text = st.text_input(label="Specify a 'regex' pattern here to limit the scope of your search", value=None, key='regex_text_input')
        st.session_state.regex_text = regex_text

        output_to_csv = st.checkbox(label="Check here to output results to a CSV file", value=False, key='output_to_csv_checkbox')
        st.session_state.output_to_csv = output_to_csv

    # Fetch the --worksheet argument
    if not state('use_previous_file_list'):
        get_worksheet_column_selection( )

    # Fetch the --tree-path argument
    get_tree( )

    # Check parameters to see if we have enough input to run a search
    go1 = state('use_previous_file_list') and state('stfs_path_selection')
    go2 = state('google_sheet_url') and state('google_worksheet_selection') and state('worksheet_column_number') and state('stfs_path_selection')

    msg = ""

    # Run a search using previous list of filenames
    if go1:
        msg = f"using the previous list of filenames AND specified directory: {state('stfs_path_selection')}"
        st.success(f"Fuzzy search is **ready**... **{msg}**")

    # Fetch new filenames for a pristine search
    elif go2:
        msg = f"using the filenames from column \'{state('worksheet_column_selection')}\' of worksheet \'{state('google_worksheet_selection')}\' AND specified directory: {state('stfs_path_selection')}"
        st.success(f"Fuzzy search is **ready**... **{msg}**")

    # Not ready for prime time
    else:
        st.warning(f"Fuzzy search parameters are incomplete!")
        st.write(f"Session state dump follows...")
        st.session_state

    # Ready... prompt for button press to run the search
    if go1 or go2:
        if st.button("Click HERE to run the search!", key='initiate_search_button'):
            with st.status(f"Go! {msg}") as status:
                csv_results = fuzzy_search_for_files(status)





