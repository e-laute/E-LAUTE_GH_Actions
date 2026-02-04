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
from utils import *

JSON_TYPE_TO_PYTHON_TYPE = {"Number": int, "String": str}


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

    active_dom, tree = parse_and_wrap_dom(filepath)

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
            raise AttributeError(f"Unknown script or wrong module path: {script}")
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

    if workpackage["commitResult"]:
        edit_appInfo(active_dom["dom"], workpackage["label"])
        with open(filepath, "wb") as f:
            tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def get_context_doms(filepath: Path):
    """
    return list of dicionaries [{filename:, notationtype:, dom:}] containing context_doms
    E-LAUTE specific implementation: context_doms are always in the same repository

    :param filepath: the filepath where to look for context_doms
    :type filepath: Path
    """

    directory = filepath.parent
    extension = ".mei"

    # 2. Find files with the same extension, excluding the original file, call wrapper
    other_files = [
        parse_and_wrap_dom(f)[0]
        for f in directory.glob(f"*{extension}")
        if f != filepath
    ]

    return other_files


def parse_and_wrap_dom(filepath: Path):
    # TODO should wrapping include filepath:Path or filename:str?
    """
    Creates wrapping {filename:, notationtype:, dom:} by parsing file

    :param filepath: The filepath of the file to be parsed and wrapped
    :type filepath: Path
    """
    tree = etree.parse(filepath, etree.XMLParser(recover=True))
    root = tree.getroot()
    filename = filepath.stem
    notationtype = determine_notationtype(filepath)
    return {
        "filename": filename,
        "dom": root,
        "notationtype": notationtype,
    }, tree


def determine_notationtype(filepath: Path):
    """
    Determines notationtype of mei.
    E-LAUTE specific implementaion: from filename (dipl|ed)_(GLT|FLT|ILT|CMN)

    :param filepath: The filepath from which to compute notation_type
    :type filepath: Path
    """
    # gets end of filename containing notationtype information
    notationtype_re = re.match(r".+_enc_((dipl|ed)_(GLT|FLT|ILT|CMN))", filepath.stem)
    if notationtype_re is None:
        raise NameError(f"{filepath.stem} doesn't fit E_LAUTE naming conventions")
    return notationtype_re.group(3)


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
    with open(Path("scripts", "work_package_example.json")) as f:
        workpackages_list = json.load(f)
    for candidate in workpackages_list:
        if candidate["id"] == args.workpackage_id:
            workpackage = candidate
            break
    if not workpackage:
        raise KeyError("Workpackage_id not found")

    dic_add_args = check_addargs_against_json(addargs_to_dic(args.addargs), workpackage)
    # hardcode 'caller-repo/' prefix to refer to caller (source) repository
    # mei_path = Path("caller-repo", filepath)
    mei_path = Path(args.filepath)
    print(f"Checking file: {mei_path}")
    if not mei_path.is_file():
        print(f"::error::File not found: '{mei_path}'")
        return 2

    # try:
    execute_workpackage(mei_path, workpackage, dic_add_args)
    print("::notice::Process completed successfully")
    return 0
    # except Exception as e:
    #   print(f"::error::Failed to process file: {e}")
    #  return 1


def check_addargs_against_json(addargs_dic: dict, workpackage: dict):
    """
    Checks parsed user input against required parameters in JSON
    Uses defaults if not provided by user

    :param addargs_dic: parsed user input
    :type addargs_dic: dict
    :param workpackage: the chosen workpackage from the JSON
    :type workpackage: dict
    """
    params = workpackage["params"]

    return_addargs = {}

    for key, value in params.items():
        if key in addargs_dic:
            if "type" not in value:
                raise KeyError(f"Parameter {key} misses type")
            try:
                return_addargs[key] = JSON_TYPE_TO_PYTHON_TYPE[value["type"]](
                    addargs_dic[key]
                )
            except ValueError as e:
                raise ValueError(
                    f"User input for {key} isn't of type {value['type']}"
                ) from e
        elif "default" in value:
            print(
                f"Warning: {key} not in additional arguments, taking default value {key}={value['default']}"
            )
            return_addargs[key] = value["default"]
        else:
            raise ValueError(f"Missing additional argument {key}")

    return return_addargs


def addargs_to_dic(addargs: list):
    """
    Parses additional argument list [key=value,key=value] to dictionary

    :param addargs: input from user as list
    :type addargs: list
    """
    kwargs = {}
    for item in addargs:
        if "=" in item:
            key, value = item.split("=", 1)  # Split only on the first '='
            kwargs[key] = value
        else:
            print(
                f"Warning: Additional argument {item} doesn't adhere to key=value format, will be ignored"
            )
    return kwargs


def initialize_parser():
    # TODO misses -nt --notationtype, -e --exclude
    parser = argparse.ArgumentParser(
        description="Coordinates the execution of scripts in the workpackage on filepath"
    )

    parser.add_argument("-f", "--filepath", help="A specific filepath")
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
