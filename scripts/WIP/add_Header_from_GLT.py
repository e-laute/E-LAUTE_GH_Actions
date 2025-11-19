import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

#TODO appinfo nicht überschreiben sondern zusammenführen

def header_from_GLT(file:str,helpfile:str):
    """adds header from GLT to CMN"""
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    if root.xpath(".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']", namespaces=ns):
        print("".join(re.search(r"Jud.*_(n\d+_).*_enc_(ed|dipl)(_CMN)",file).groups()),"hat schon header")
        return

    with open(helpfile, "rb") as h:
        helptree = ET.parse(h,ET.XMLParser(recover=True))
    helproot = helptree.getroot()

    if not helproot.xpath(".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']", namespaces=ns):
        print("".join(re.search(r"Jud.*_(n\d+_).*_enc_(ed|dipl)(_GLT)",helpfile).groups()),"hat keinen header")
        return

    appInfo = root.find(".//mei:appInfo", namespaces=ns)

    header = copy.deepcopy(helproot.find(".//mei:meiHead", namespaces=ns))

    titlePart = header.find(".//mei:titlePart/mei:abbr", namespaces=ns)
    titlePart.clear()
    titlePart.set("expan","Common Music Notation")
    titlePart.text="CMN"

    edition = header.find(".//mei:edition", namespaces=ns)
    edition.set("resp","#projectstaff-16")
    edition.text = f"First {"diplomatic transcription" if "dipl" in file else "edition"} in CMN. Lute tuned in A."

    judenkunig = header.xpath(".//mei:persName[@corresp='#persons-78']",namespaces=ns)
    if judenkunig:
        judenkunig[0].set("role","intabulator")

    appinfoold = header.find(".//mei:appInfo",namespaces=ns)
    encodingDesc = appinfoold.getparent()
    encodingDesc.remove(appinfoold)
    encodingDesc.insert(0,appInfo)

    edps = header.xpath(".//mei:editorialDecl//p",namespaces=ns)

    for edp in edps:
        edp.text=""

    revisionDesc = header.find("./mei:revisionDesc",namespaces=ns)
    del revisionDesc[1:]
    revisionDesc[0].attrib.update({"isodate":"YYYY-MM-DD", "n":"1", "resp":"#"})
    revps = revisionDesc.xpath(".//mei:p",namespaces=ns)

    for revp in revps:
        revp.text=""

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
            if re.fullmatch(r"Jud.*_enc_(ed|dipl)_CMN\.mei",file)!=None:
                header_from_GLT(os.path.join(root,file),os.path.join(root,re.sub(r"(.*)_enc_(ed|dipl)_CMN(\.mei)",r"\1_enc_\2_GLT\3",file)))

choosefile()