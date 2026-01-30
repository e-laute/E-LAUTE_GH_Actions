"""
Coordinates the workpackage with the associated file(s)
"""

import sys
import os
import argparse
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
    "-w", "--workpackage", required=True, help="The workpackage to be executed"
)
parser.add_argument(
    "-a",
    "--addargs",
    nargs="*",
    help="Additional arguments required by the workpackage, formatted key=value",
)

with open("workpackages.json") as f:
    workpackages_dic = json.load(f)


def execute_workpackage(filepath: str, workpackage: dict, addargs: dict):
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
        raise Exception("Faulty workpackage, missing 'scripts'")

    with open(filepath) as f:
        tree = etree.parse(f, etree.XMLParser(recover=True))
    root = tree.getroot()

    # TODO differentiate sibling type
    if workpackage["sibling"]:
        sibling_root = getsibling(filepath)
    else:
        sibling_root = None

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
        raise Exception("Unknown module")

    for script in scripts_list:
        module_path, _dot, func_name = script.rpartition(".")
        current_func = getattr(modules_dic[module_path], func_name, None)
        if current_func is None:
            raise Exception(f"Unknown script or wrong module path: {script}")
        # TODO currently scripts need to handle all input
        root = current_func(root, sibling_root, addargs)


def getsibling(filepath):
    pass


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

    try:
        workpackage = workpackages_dic[
            args.workpackage
        ]  # argv needs to contain workpackage
    except KeyError:
        print(f"::error::Unknown workpackage: {args.workpackage}")
        return 1

    files = []
    if args.filepath:
        files.append(args.file)
    else:
        files = get_file_from_id(args.include)

    for filepath in files:
        # hardcode 'caller-repo/' prefix to refer to caller (source) repository
        mei_path = os.path.join("caller-repo", filepath)
        print(f"Checking file: {mei_path}")
        if not os.path.isfile(mei_path):
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
