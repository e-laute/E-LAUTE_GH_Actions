"""
Coordinates the workpackage with the associated file(s)
"""

import argparse
import importlib
import json
import re
import sys
from pathlib import Path

from lxml import etree


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
    except KeyError as e:
        raise KeyError("Faulty workpackage, missing 'scripts'") from e

    active_dom = parse_and_wrap_dom(filepath)

    # TODO differentiate sibling type
    context_doms = get_context_doms(filepath)

    # scripts in the JSON is a list of module to function paths (dir.subdir.module.func)
    # modules_dic contains the path of the module as key (dir.subdir.module) and the loaded module as item
    modules_list = list(set([script.rpartition(".")[0] for script in scripts_list]))
    try:
        modules_dic = {mod: importlib.import_module(mod) for mod in modules_list}
    except ImportError as e:
        raise NameError("Unknown module") from e

    for script in scripts_list:
        module_path, _dot, func_name = script.rpartition(".")
        current_func = getattr(modules_dic[module_path], func_name, None)
        if current_func is None:
            raise Exception(f"Unknown script or wrong module path: {script}")
        # scripts take active_dom:dict, context_dom:list[dict], params:dict
        try:
            active_dom = current_func(active_dom, context_doms, **params)
        except TypeError as e:
            # "...missing 1 required positional argument: 'x'" -> params was missing key
            if "missing" in str(e):
                # Extract argument names inside single quotes
                missing_names = re.findall(r"'(.*?)'", str(e))
                arg_list = ", ".join(missing_names)
                raise KeyError(
                    f"The additional arguments passed are incomplete, {func_name} requires: {arg_list}"
                ) from e
            else:
                # If it's a different kind of TypeError, re-raise it
                raise e


def get_context_doms(filepath: Path):
    """return list of dicionaries [{filename:, notationtype:, dom:}]
    E-LAUTE specific implementation: context_doms are always in the same repository"""

    directory = filepath.parent
    extension = filepath.suffix  # should be ".mei"

    # 2. Find files with the same extension, excluding the original file, call wrapper
    other_files = [
        parse_and_wrap_dom(f) for f in directory.glob(f"*{extension}") if f != filepath
    ]

    return other_files


def parse_and_wrap_dom(filepath: Path):
    # TODO should wrapping include filepath:Path or filename:str?
    """
    Creates wrapping {filename:, notationtype:, dom:} by parsing file

    :param filepath: Description
    :type filepath: Path
    """
    tree = etree.parse(filepath, etree.XMLParser(recover=True))
    root = tree.getroot()
    filename = filepath.stem
    notationtype = determine_notationtype(filepath)
    return {"filename": filename, "dom": root, "notationtype": notationtype}


def determine_notationtype(filepath: Path):
    # TODO
    return "ed_CMN"


def main():
    """
    Parses Arguments, selects file, calls coordinator on files with workpackage
    """
    # TODO misses -nt --notationtype, -e --exclude
    # For now assumes python coordinator.py filepath workpackage additional arguments
    # TODO check for validity of workpackage x filetype, multiple files
    parser = initialize_parser()
    args = parser.parse_args()

    # TODO specify as arg
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
        # mei_path = Path("caller-repo", filepath)
        mei_path = Path(filepath)
        print(f"Checking file: {mei_path}")
        if not mei_path.is_file():
            print(f"::error::File not found: '{mei_path}'")
            return 2

        try:
            execute_workpackage(mei_path, workpackage, addargs_to_dic(args.addargs))
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


def initialize_parser():
    # TODO misses -nt --notationtype, -e --exclude
    parser = argparse.ArgumentParser(
        description="Coordinates the execution of scripts in the workpackage on filepath"
    )

    include = parser.add_mutually_exclusive_group(required=True)
    include.add_argument(
        "-i", "--include", nargs="*", help="Included files by id number"
    )
    include.add_argument("-f", "--filepath", help="A specific filepath")
    parser.add_argument(
        "-w",
        "--workpackage_id",
        required=True,
        help="The id of the workpackage to be executed",
    )
    parser.add_argument(
        "-a",
        "--addargs",
        nargs="*",
        help="Additional arguments required by the workpackage, formatted key=value",
    )
    return parser


if __name__ == "__main__":
    sys.exit(main())
