import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}


def mensur_and_brace(file:str,helpfile:str):

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    with open(helpfile, "rb") as h:
        helptree = ET.parse(h,ET.XMLParser(recover=True))
    helproot = helptree.getroot()

    grpSym = root.xpath("//mei:grpSym", namespaces=ns)[0]

    grpSym.set("symbol","brace")

    meterSigs = root.xpath("//mei:meterSig", namespaces=ns)

    for meterSig in meterSigs:
        meterSig.set("enclose","brack")
        meterSig.attrib.pop("visible",None)

    mensur = helproot.xpath("//mei:mensur", namespaces=ns)
    oldmensur = root.xpath("//mei:mensur", namespaces=ns)

    if mensur and not oldmensur:
        newmensur = copy.deepcopy(mensur[0])
        newmensur.attrib.pop("{http://www.w3.org/XML/1998/namespace}id",None)
        staffdefs = root.xpath("//mei:staffDef", namespaces=ns)
        if len(staffdefs) < 2:
            print(f"not enough staffdef for {file}")
            return
        staffdefs[0].append(newmensur)
        staffdefs[1].append(copy.deepcopy(newmensur))

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
        

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei",file)!=None:
                mensur_and_brace(os.path.join(root,file),os.path.join(root,re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))

choosefile()