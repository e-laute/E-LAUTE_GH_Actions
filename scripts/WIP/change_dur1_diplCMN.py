import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace",
        "re":"http://exslt.org/regular-expressions"}


def dur1_to_dur2(file:str):
    """changes dur=1 to dur=2"""

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    chords = root.xpath(".//mei:chord[@dur='1']", namespaces=ns)

    for chord in chords:
        if chord.get("type") is not None:
            print(f"Chord {chord.attrib} in {file} has type")
            continue
        chord.set("dur","2")
        chord.set("type","dur1")

        
    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])   

    ET.indent(tree,"   ")     

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def dur2_to_dur1(file:str):
    """changes type=dur1 to dur=2"""

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    chords = root.xpath(".//mei:chord[@type='dur1']", namespaces=ns)

    for chord in chords:
        chord.set("dur","1")

        
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
            if re.fullmatch(r"Jud.+n\d+.+_(dipl)_CMN\.mei",file)!=None:
                dur1_to_dur2(os.path.join(root,file))


choosefile()