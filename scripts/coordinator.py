"""
Coordinates the workpackage with the associated file(s)
"""

import sys
import os
import argparse
from pathlib import Path
import importlib
import json
from lxml import etree

# initializing
parser = argparse.ArgumentParser(
    description="Coordinates the execution of scripts in the workpackage on filepath"
)
include = parser.add_mutually_exclusive_group(required=True)
include.add_argument(
    "-i", "--include", nargs="*", help="Included files by id number"
)
include.add_argument("-f", "--filepath", help="A specific filepath")
parser.add_argument(
    "-w", "--workpackage_id", required=True, help="The id of the workpackage to be executed"
)
parser.add_argument(
    "-a",
    "--addargs",
    nargs="*",
    help="Additional arguments required by the workpackage, formatted key=value",
)




def execute_workpackage(filepath: Path, workpackage: dict, params: dict):
    """
    Parses filepath, loads the specefied workpackage from workpath
    and calls the designated scripts on the parsed file.

    :param filepath: the file to be processed
    :type filepath: str
    :param workpackage: the workpackage to be executed
    :type workpackage: dict
    :param addargs: arguments required for the workpackage
    """
    try:
        scripts_list = workpackage["scripts"]
    except KeyError:
        raise KeyError("Faulty workpackage, missing 'scripts'")

    active_dom = parse_and_wrap_dom(filepath)

    # TODO differentiate sibling type
    context_doms = get_context_doms(filepath)

    # scripts in the JSON is a list of module to function paths (dir.subdir.module.func)
    # modules_dic contains the path of the module as key (dir.subdir.module) and the loaded module as item
    modules_list = list(
        set([script.rpartition(".")[0] for script in scripts_list])
    )
    try:
        modules_dic = {
            mod: importlib.import_module(mod) for mod in modules_list
        }
    except ImportError:
        raise NameError("Unknown module")

    for script in scripts_list:
        module_path, _dot, func_name = script.rpartition(".")
        current_func = getattr(modules_dic[module_path], func_name, None)
        if current_func is None:
            raise Exception(f"Unknown script or wrong module path: {script}")
        # TODO currently scripts need to handle all input
        active_dom = current_func(params, active_dom, context_doms)


def get_context_doms(filepath:Path):
    """return list of dicionaries [{filename:, notationtype:, dom:}]"""


    return []
    parse_and_wrap_dom(sibling_path)
    pass

def parse_and_wrap_dom(filepath:Path):
    tree = etree.parse(filepath, etree.XMLParser(recover=True))
    root = tree.getroot()
    filename = filepath.stem
    notationtype = determine_notationtype(filepath)
    return {"filename":filename,"dom":root,"notationtype":notationtype}

def determine_notationtype(filepath:Path):
    #TODO
    return "ed_CMN"

def main():
    """
    Parses Arguments, selects file, calls coordinator on files with workpackage
    """
    # TODO needs to be rewritten to fit new design
    # TODO needs to handle arguments passed, maybe argparse?
    # For now assumes python coordinator.py filepath workpackage additional arguments
    # TODO needs to be able to handle multiple files
    # TODO check for validity of workpackage x filetype, multiple files
    args = parser.parse_args()

    #TODO specify as arg
    with open("work_package_example.json") as f:
        workpackages_list = json.load(f)
    for canditate in workpackages_list:
        if canditate["id"] == args.workpackage_id:
            workpackage = canditate
            break
    if not workpackage:
        raise KeyError("Workpackage_id not found")

    files = []
    if args.filepath:
        files.append(args.filepath)
    else:
        files = get_file_from_id(args.include)

    for filepath in files:
        # hardcode 'caller-repo/' prefix to refer to caller (source) repository
        #mei_path = Path("caller-repo", filepath)
        mei_path = Path(filepath)
        print(f"Checking file: {mei_path}")
        if not mei_path.is_file():
            print(f"::error::File not found: '{mei_path}'")
            return 2

        try:
            execute_workpackage(
                mei_path, workpackage, addargs_to_dic(args.addargs)
            )
            print("::notice::Process completed successfully")
            return 0
        except Exception as e:
            print(f"::error::Failed to process file: {e}")
            return 1


def get_file_from_id(*args):
    pass


def addargs_to_dic(addargs: list):
    kwargs = {}
    for item in addargs:
        if "=" in item:
            key, value = item.split("=", 1)  # Split only on the first '='
            kwargs[key] = value
    return kwargs


if __name__ == "__main__":
    sys.exit(main())
