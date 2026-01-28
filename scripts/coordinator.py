"""
coordinates the workpatch with the associated file(s)
"""

import os
import re
import copy
import sys
from pathlib import Path
import importlib
import json
from lxml import etree

#initializing
with open("workpatches.json") as f:
    workpatches_dic = json.load(f)

def execute_workpatch(filepath:str, workpatch:dict, *addargs): #TODO **addargs, main needs better argument handling
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

def main(argv: list[str]):
    """
    Handles arguments passed from yaml
    
    :param argv: arguments passed from yaml
    :type argv: list[str]
    """
    #TODO needs to be rewritten to fit new design
    #TODO needs to handle arguments passed, maybe argparse? 
    #For now assumes python coordinator.py filepath workpatch additional arguments
    #TODO needs to be able to handle multiple files
    #TODO check for validity of workpatch x filetype, multiple files
    if len(argv) != 2 or argv[0] in {"-h", "--help"}:
        print(__doc__.strip())
        print("Received inputs:", argv)
        return 1
    # hardcode 'caller-repo/' prefix to refer to caller (source) repository
    mei_path = Path('caller-repo') / Path(argv[1])
    print(f"Checking file: {mei_path}")
    
    if not mei_path.is_file():
        print(f"::error::File not found: '{mei_path}'")
        return 2
    

    try:
        workpatch = workpatches_dic[argv[2]] #argv needs to contain workpatch
    except KeyError:
        print(f"::error::Unknown workpatch: {argv[2]}")
        return 1
    
    try:
        execute_workpatch(mei_path,workpatch,argv[3:])
        print("::notice::Process completed successfully")
        return 0
    except Exception as e:
        print(f"::error::Failed to process file: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv)) 