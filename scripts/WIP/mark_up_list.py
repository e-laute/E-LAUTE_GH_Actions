import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

def handle_staffDef(controlelem:ET.Element):
    print(f"Missing Missing equivilant for {ET.QName(controlelem).localname} (xml:id={controlelem.get("{http://www.w3.org/XML/1998/namespace}id")}) in StaffDef")

def find_compare(controlelem:ET.Element,origroot:ET.Element):
    """Finds corresponding element in helproot"""
    look_outside=False
    contmeasures=controlelem.xpath(".//mei:measure",namespaces=ns)
    if contmeasures:
        contmeasure=contmeasures[0]
        look_outside=True
    elif controlelem.xpath(".//mei:fing",namespaces=ns):
        return True,0 #fing ist direkt unter measure, vorher alle choice die measure enthalten, hier also nur noch fing direkt
    else:
        tempelem=controlelem
        while tempelem.tag != "{http://www.music-encoding.org/ns/mei}measure":
            #print("\nTempelem-Test")
            #print(tempelem.attrib,ET.QName(tempelem).localname)
            tempelem=tempelem.getparent()
            if tempelem.tag=="{http://www.music-encoding.org/ns/mei}staffdef":
                return handle_staffDef(controlelem)
        contmeasure=tempelem
    
    mnum=contmeasure.get("n")
    #print(mnum,contmeasure.attrib)

    rootmeasure=origroot.xpath(f"//mei:measure[@n={mnum}]",namespaces=ns)
    if len(rootmeasure)!=1 and not look_outside:
        print(f"Measure with @n={mnum} either missing or multiple in CMN")
        [print(rm.attrib) for rm in rootmeasure]
        return False,0
    
    if look_outside:
        rootelem=rootmeasure[0].getparent()
        return rootelem.tag == controlelem.tag, mnum
    else:
        #If there are at least as many mark-up elems of a certain type in measure_CMN as in measure_GLT
        #it's possible (NOT GUARANTEED) that every mark up has a counter part, otherwise impossible
        contelemtag = ET.QName(controlelem).localname
        rootelems=rootmeasure[0].xpath(f".//mei:{contelemtag}",namespaces=ns)
        return len(rootelems) >= len(contmeasure.xpath(f".//mei:{contelemtag}",namespaces=ns)), mnum




def mark_up_list(file:str,helpfile:str):
    """creates list of mark-up in GLT not found in CMN"""

    mark_up_li = []
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    with open(helpfile, "rb") as h:
        helptree = ET.parse(h,ET.XMLParser(recover=True))
    helproot = helptree.getroot()

    choices = helproot.xpath(".//mei:music//mei:choice", namespaces=ns)
    for choice in choices:
        equi, mnum= find_compare(choice,root)
        if not equi:
            mark_up_li.append((choice,mnum))

    supplieds = helproot.xpath(".//mei:music//mei:supplied", namespaces=ns)
    for supplied in supplieds:
        equi, mnum= find_compare(supplied,root)
        if not equi:
            mark_up_li.append((supplied,mnum))
    
    return mark_up_li

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei",file)!=None:
                mark_up_li=mark_up_list(os.path.join(root,file),os.path.join(root,re.sub(r"(.*)_enc_.*_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))
                if mark_up_li:
                    print("\n"+file)
                    [print(f"Missing equivilant for {ET.QName(mark_up[0]).localname} (xml:id={mark_up[0].get("{http://www.w3.org/XML/1998/namespace}id")}) at mnum {mark_up[1]}") for mark_up in mark_up_li]

choosefile()