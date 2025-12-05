import os
import re
from lxml import etree as ET
import copy
import sys
import math


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

def dur_length(elem:ET.Element):
    totaldur = 0.
    for child in elem:
        if "dur" in child.attrib:
            dur = float(child.attrib.get("dur"))
            totaldur += 2/dur - 1/(dur*2**int(child.attrib.get("dots","0")))
        elif child:
            totaldur+=dur_length(child)
    return totaldur

def combine_elems(elem1:ET.Element,elem2:ET.Element,ignore:tuple=()):
    add_tstamp=dur_length(elem1)*4
    for child in list(elem2):
        if child.tag in ignore:
            continue
        tstamps = child.xpath(".//mei:*[@tstamp]|self::mei:*[@tstamp]",namespaces=ns)
        for tstamp in tstamps:
            tstamp.set("tstamp",str(float(tstamp.get("tstamp"))+add_tstamp))
        elem1.append(child)

def combine_measure(file:str):

    # Re-open to parse full document
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    first_measures = root.xpath(".//mei:measure[@right='invis']",namespaces=ns)

    for first_measure in first_measures:
        print(first_measure.attrib)
        n = first_measure.get("n")
        if "a" in n:
            n = n[:-1] + 'b'
        second_measures = root.xpath(f".//mei:measure[@n='{n}']",namespaces=ns)
        if second_measures and 'b' in n:
            second_measure = second_measures[0]
        elif second_measures:
            second_measure = second_measures[1]
        else:
            print(f"file has problem at n={first_measure.get("n")}")
            return
        
        new_measure=copy.deepcopy(first_measure)
        combine_elems(new_measure,second_measure,(f"{{{ns['mei']}}}staff",))
        combine_elems(new_measure.find(".//mei:layer",namespaces=ns),second_measure.find(".//mei:layer",namespaces=ns))
        new_measure.set("n",new_measure.get("n")[:-1])
        if second_measure.get("right") is not None:
            new_measure.set("right", second_measure.get("right"))
        else:
            new_measure.attrib.pop("right")
        new_measure.attrib.pop("metcon")

        first_measure.getparent().remove(first_measure)
        parent = second_measure.getparent()
        parent.remove(second_measure)
        parent.insert(0,new_measure)

    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])

    ET.indent(tree,"   ")

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