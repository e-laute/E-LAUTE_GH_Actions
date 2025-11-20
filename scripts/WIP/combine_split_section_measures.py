import os
import re
from lxml import etree as ET
import copy
import sys
import math


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

    

def combine_measure(file:str):

    # Re-open to parse full document
    with open(file, "rb") as f:
        tree = ET.parse(f)
    root = tree.getroot()

    first_measures = root.xpath(".//mei:measure[substring(@n,string-length(@n)-1)='a']")

    for fm in first_measures:
        print(fm.attrib)

    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])        

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

def changefilename():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r".*\d+(r|v)-\d+(r|v)_enc_ed_GLT\.mei",file)!=None:
                combine_measure(os.path.join(root,file))

changefilename()