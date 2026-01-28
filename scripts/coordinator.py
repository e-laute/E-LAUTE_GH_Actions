"""
coordinates the workpatch with the associated file(s)
"""

import os
import re
import copy
import sys
from pathlib import Path
import argparse
import importlib
import json
from lxml import etree

#initializing
parser =argparse.ArgumentParser(description="Coordinates the execution of scripts in the workpatch on filepath")
include = parser.add_mutually_exclusive_group(required=True)
include.add_argument("-i","--include",nargs="*", help="Included files by id number")
include.add_argument("-f","--filepath",help="A specific filepath")
parser.add_argument("-w","--workpatch",required=True, help="The workpatch to be executed")
parser.add_argument("-a","--addargs",nargs="*", help="Additional arguments required by the workpatch, formatted key=value")

with open("workpatches.json") as f:
    workpatches_dic = json.load(f)

def execute_workpatch(filepath:str, workpatch:dict, addargs:dict):
    """
    Parses filepath, loads the specefied workpatch from workpath 
    and calls the designated scripts on the parsed file.
    
    :param filepath: the file to be processed
    :type filepath: str
    :param workpatch: the workpatch to be executed
    :type workpatch: dict
    :param addargs: arguments required for the workpatch
    """
    try:
        scripts_li = workpatch["scripts"]
    except KeyError:
        raise Exception("Faulty workpatch, missing 'scripts'")
    
    with open(filepath) as f:
        tree = etree.parse(f,etree.XMLParser(recover=True))
    root = tree.getroot()

    #TODO differentiate sibling type
    if workpatch["sibling"]:
        sibling_root = getsibling(filepath)
    else:
        sibling_root = None

    #scripts in the JSON is a list of module to function paths (dir.subdir.module.func)
    #modules_dic contains the path of the module as key (dir.subdir.module) and the loaded module as item
    modules_li = list(set([script.rpartition(".")[0] for script in scripts_li]))
    try:
        modules_dic = {mod:importlib.import_module(mod) for mod in modules_li}
    except ImportError:
        raise Exception("Unknown module")

    for script in scripts_li:
        module_path, dot, func_name = script.rpartition(".")
        current_func = getattr(modules_dic[module_path],func_name,None)
        if current_func is None:
            raise Exception(f"Unknown script or wrong module path: {script}")
        #TODO currently scripts need to handle all input
        root = current_func(root,sibling_root,addargs)

def getsibling(filepath):
    pass

def main():
    """
    Parses Arguments, selects file, calls coordinator on files with workpatch 
    """
    #TODO needs to be rewritten to fit new design
    #TODO needs to handle arguments passed, maybe argparse? 
    #For now assumes python coordinator.py filepath workpatch additional arguments
    #TODO needs to be able to handle multiple files
    #TODO check for validity of workpatch x filetype, multiple files
    args = parser.parse_args()
    

    try:
        workpatch = workpatches_dic[args.workpatch] #argv needs to contain workpatch
    except KeyError:
        print(f"::error::Unknown workpatch: {args.workpatch}")
        return 1

    
    files = []
    if args.filepath:
        files.append(args.file)
    else:
        files = get_file_from_num(args.include)
    
    for filepath in files:
        # hardcode 'caller-repo/' prefix to refer to caller (source) repository
        mei_path = Path('caller-repo') / Path(filepath)
        print(f"Checking file: {mei_path}")
        if not mei_path.is_file():
            print(f"::error::File not found: '{mei_path}'")
            return 2
        
        try:
            execute_workpatch(mei_path,workpatch,addargs_to_dic(args.addargs))
            print("::notice::Process completed successfully")
            return 0
        except Exception as e:
            print(f"::error::Failed to process file: {e}")
            return 1
    
def get_file_from_num(*args):
    pass

def addargs_to_dic(addargs:list):
    kwargs = {}
    for item in addargs:
        if '=' in item:
            key, value = item.split('=', 1) # Split only on the first '='
            kwargs[key] = value
    return kwargs

if __name__ == "__main__":
    sys.exit(main()) 