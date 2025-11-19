import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}



def move_cert(file:str):
    """moves cert from choice to corr"""
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()


    #remove before adding again
    choices = root.xpath("//mei:choice[@cert]/mei:corr/..", namespaces=ns)
    if choices:
        print(choices)

    for choice in choices:
        corr = choice.find("./mei:corr", namespaces=ns)
        cert = choice.pop("cert")
        if corr.get("cert") is None:
            corr.set("cert",cert)
        elif corr.get("cert") != cert:
            print(f"{file} has choice with two certs at id {choice.get(f"{{{ns['xml']}}}id","no xml idea")} with choice cert = {cert} and corr cert = {corr.get("cert")}")


    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])
    ET.indent(tree,"   ")

    #change processing instructiuns and version to mei 5.1
        

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(ed|dipl)_(CMN|GLT)\.mei",file)!=None:
                move_cert(os.path.join(root,file))

choosefile()