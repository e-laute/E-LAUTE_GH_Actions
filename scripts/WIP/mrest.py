import os
import re
from lxml import etree as ET
import copy
import sys
import math


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

    

def resttomrest(file:str):

    # Re-open to parse full document
    with open(file, "rb") as f:
        tree = ET.parse(f)
    root = tree.getroot()

    metersig_list = root.xpath(".//mei:meterSig", namespaces=ns)

    if metersig_list:
        metersig=metersig_list[0]
    else:
        return

    unit=float(metersig.attrib.get("unit",0))
    count=float(metersig.attrib.get("count",1))
    dur = math.ceil(unit/count)
    if unit%count:
        rests = root.xpath(f"//mei:rest[@dur='{dur}'][@dots]", namespaces=ns)
    else:
        rests = root.xpath(f"//mei:rest[@dur='{dur}'][not(@dots)]", namespaces=ns)


    for rest in rests:
        print(rest.tag)
        parent = rest.getparent()
        if len(parent) == 1:  # only child in its parent
            # Rename to <mRest>
            rest.tag = "{http://www.music-encoding.org/ns/mei}mRest"

            # Keep only globally valid attributes
            xml_id = rest.attrib.get("{http://www.w3.org/XML/1998/namespace}:id")
            rest.attrib.clear()
            if xml_id:
                rest.attrib["{http://www.w3.org/XML/1998/namespace}:id"] = xml_id
            

    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])        

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

def changefilename():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r".*_enc_ed_CMN\.mei",file)!=None:
                print(os.path.join(root,file))
                resttomrest(os.path.join(root,file))

changefilename()