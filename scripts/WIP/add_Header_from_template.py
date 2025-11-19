import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

template = {"dipl_GLT":"templates\\Jud_1523-2_meiHead_dipl.xml",
            "dipl_CMN":"templates\\Jud_1523-2_meiHead_dipl_CMN.xml",
            "ed_GLT":"templates\\Jud_1523-2_meiHead_ed.xml",
            "ed_CMN":"templates\\Jud_1523-2_meiHead_ed_CMN.xml",
            }



def header_from_template(file:str):
    """adds header from GLT to CMN"""
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    if root.xpath(".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']", namespaces=ns):
        return

    shortname="".join(re.search(r"Jud.*_enc_(ed|dipl)(_GLT|_CMN)",file).groups())

    with open(template[shortname], "rb") as h:
        helptree = ET.parse(h,ET.XMLParser(recover=True))
    helproot = helptree.getroot()

    appInfo = root.find(".//mei:appInfo", namespaces=ns)

    header = copy.deepcopy(helproot.find(".//mei:meiHead", namespaces=ns))

    appinfoold = header.find(".//mei:appInfo",namespaces=ns)
    encodingDesc = appinfoold.getparent()
    encodingDesc.remove(appinfoold)
    encodingDesc.insert(0,appInfo)

    root.remove(root.find("./mei:meiHead",namespaces=ns))
    root.insert(0,header)

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
            if re.fullmatch(r"Jud.*_enc_(ed|dipl)_(GLT|CMN)\.mei",file)!=None:
                header_from_template(os.path.join(root,file))

choosefile()