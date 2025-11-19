import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}



def annot_list(file:str):
    """creates list of annots not in choice"""

    annot_li = []
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    annots = root.xpath(".//mei:music//mei:annot", namespaces=ns)
    for annot in annots:
        if ET.QName(annot.getparent()).localname != "choice":
            annot_li.append(annot.get(f"{{{ns['xml']}}}id","unkown_id"))
    
    return annot_li

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei",file)!=None:
                annot_li=annot_list(os.path.join(root,file))
                if annot_li:
                    print("\n"+file)
                    [print(f"Annot with (xml:id={annot}) not in choice") for annot in annot_li]

choosefile()