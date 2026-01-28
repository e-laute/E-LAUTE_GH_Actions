import importlib
import json
from lxml import etree

def setup(filepath:str, workpatch:str, **addargs):

    with open("workpatches.json") as f:
        workpatches_dic = json.load(f)
    try:
        scripts_li = workpatches_dic[workpatch]["scripts"]
    except KeyError:
        raise Exception("Unknown or faulty workpatch")
    
    with open(filepath) as f:
        tree = etree.parse(f,etree.XMLParser(recover=True))
    root = tree.getroot()

    if workpatches_dic[workpatch]["sibling"]:
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
        root = current_func(root,sibling_root,addargs)