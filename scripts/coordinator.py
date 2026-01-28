import json
from lxml import etree
import script_collection

def setup(filepath:str, workpatch:str, **addargs):

    with open("workpatches.json") as f:
        workpatches_dic = json.load(f)
    try:
        sripts_li = workpatches_dic[workpatch]["scripts"]
    except KeyError:
        raise Exception("Unknown or faulty workpatch")
    
    with open(filepath) as f:
        tree = etree.parse(f,etree.XMLParser(recover=True))
    root = tree.getroot()

    if workpatches_dic[workpatch]["sibling"]:
        sibling_root = getsibling(filepath)

    
    for script in sripts_li:
        current_func = getattr(script_collection,script)
        root = current_func(root,sibling_root,**addargs)