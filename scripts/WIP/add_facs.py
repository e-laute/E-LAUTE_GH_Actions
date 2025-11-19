import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}



def add_facs(file:str,helpfile:str):
    """adds facs from GLT to CMN"""

    print(helpfile)
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    with open(helpfile, "rb") as h:
        helptree = ET.parse(h,ET.XMLParser(recover=True))
    helproot = helptree.getroot()

    music = root.xpath("//mei:music", namespaces=ns)[0]
    for child in music:
        print(child.tag)

    oldfacs=root.xpath("//mei:facsimile", namespaces=ns)

    if bool(oldfacs):
        return

    facs = helproot.xpath("//mei:facsimile", namespaces=ns)

    if facs:
        newfacs = copy.deepcopy(facs[0])
        newfacs.attrib.pop("{http://www.w3.org/XML/1998/namespace}id",None)
        graphics = newfacs.findall(".//mei:graphic",namespaces=ns)
        for graph in graphics:
            graph.attrib.pop("{http://www.w3.org/XML/1998/namespace}id",None)

        music.insert(0,newfacs)
        for child in music:
            print(child.tag)



    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])

    #change processing instructiuns and version to mei 5.1

    root.set("meiversion","5.1")
    for pi in tree.xpath("//processing-instruction()"):
        if pi.target == "xml-model":
            pi.text = re.sub(
            r'href="[^"]+"',
            'href="https://music-encoding.org/schema/5.1/mei-all.rng"',
            pi.text
            )
        
    ET.indent(tree,"   ")
    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei",file)!=None:
                add_facs(os.path.join(root,file),os.path.join(root,re.sub(r"(.*)_enc_.*_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))

choosefile()