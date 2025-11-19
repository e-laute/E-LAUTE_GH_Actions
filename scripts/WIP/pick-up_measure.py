import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

def make_pickup(fmeasure:ET.Element,root:ET.Element):
    fmeasure.set("type","pickup")
    fmeasure.set("metcon","false")
    if fmeasure.get("n")=="0":
        return
    if fmeasure.get("n")!="1":
        print("check mnum of first measure")
        return
    measures=root.xpath("//mei:measure",namespaces=ns)
    for measure in measures:
        try:
            measure.set("n",str(int(measure.get("n"))-1))
        except:
            print(f"check mnum of measure with id {measure.get("{http://www.w3.org/XML/1998/namespace}id")}")
    return


def pick_up_check(file:str):

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    first_measure = root.find(".//mei:measure", namespaces=ns)

    layers = first_measure.xpath(".//mei:layer", namespaces=ns)

    for layer in layers:
        if not ET.QName(layer[0]).localname in ["rest","mRest","space"]:
            metersigs = root.xpath("//mei:meterSig",namespaces=ns)
            if first_measure.get("metcon") == "false" and first_measure.get("type","None") != "pickup" and metersigs:
                print(f"changing first measure in {file} to pickup")
                make_pickup(first_measure,root)
            break
    else:
        print(f"first measure of {re.search(r"_(n\d+)",file).group(1)} starts only with breaks, changing to pickup")
        make_pickup(first_measure,root)
        
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
                pick_up_check(os.path.join(root,file))

choosefile()