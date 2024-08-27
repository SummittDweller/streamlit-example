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

import streamlit as st
import tkinter as tk
from tkinter import filedialog


# Modules defined and used in https://medium.com/@kjavaman12/how-to-create-a-folder-selector-in-streamlit-e44816c06afd
# ---------------------------------------------------------------------

def select_folder( ):
    root = tk.Tk( )
    root.withdraw( )
    folder_path = filedialog.askdirectory(master=root)
    root.destroy( )
    return folder_path


# test( )
# ---------------------------------------------------------------------
def test( ):
    selected_folder_path = st.session_state.get("folder_path", None)
    folder_select_button = st.button("Select Folder")
    if folder_select_button:
        selected_folder_path = select_folder( )
        st.session_state.folder_path = selected_folder_path

    if selected_folder_path:
        st.write("Selected folder path:", selected_folder_path)

# ----------------------------------------------------------------------        
# --- Main

if __name__ == '__main__':

    # --tree-path
    test( )



