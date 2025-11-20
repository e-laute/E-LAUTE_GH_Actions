import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}


def measure_length(elem:ET.Element):
    tstamp = 0.
    for child in elem:
        print(child.tag)
        if child.tag == "{http://www.music-encoding.org/ns/mei}beam":
            tstamp+=measure_length(child)
        if child.tag == "{http://www.music-encoding.org/ns/mei}note" or child.tag == "{http://www.music-encoding.org/ns/mei}chord" or child.tag == "{http://www.music-encoding.org/ns/mei}tabGrp":
            print(tstamp)
            dur=float(child.attrib.get("dur"))
            tstamp += 2/dur - 1/(dur*2**int(child.attrib.get("dots",0)))
    return tstamp




def add_finis(file:str):
    """adds finis on the last tstamp+1 of the last measure, doesnt account for mark-up or n-tuplets"""
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()


    #remove before adding again
    finis = root.xpath("//mei:dir[@type='finis']", namespaces=ns)

    if finis:
        return
    
    meterSig = root.find(".//mei:meterSig", namespaces=ns)

    measure = root.xpath("//mei:measure", namespaces=ns)[-1]
    layer = measure.find(".//mei:layer", namespaces=ns)
    tstamp = measure_length(layer)*int(meterSig.get("unit","4"))
    
    #for fin in finis:
    #    fin.set("tstamp",str(tstamp))

    #if not finis:
    dir = ET.SubElement(measure,"dir",{"staff":"2", "tstamp":str(tstamp), "place":"above", "type":"finis"})
    dir.text = "Finis"

    print(f"added finis to {file}")


    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])
    ET.indent(tree,"   ")

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
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r".*_enc_ed_CMN\.mei",file)!=None:
                add_finis(os.path.join(root,file))

choosefile()