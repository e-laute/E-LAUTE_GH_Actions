import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace",
        "re":"http://exslt.org/regular-expressions"}


def fol_dir_fontsize(file:str):
    """changtes fontesize of foldir to x-small"""

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    candidates = root.xpath(".//mei:rend", namespaces=ns)
    foldirs=[el for el in candidates if re.match(r"fol\. \d+[rv]", el.text or "")]

    for foldir in foldirs:
        foldir.set("fontsize","x-small")
        
    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])   

    ET.indent(tree,"   ")     

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(ed|dipl)_CMN\.mei",file)!=None:
                fol_dir_fontsize(os.path.join(root,file))


choosefile()