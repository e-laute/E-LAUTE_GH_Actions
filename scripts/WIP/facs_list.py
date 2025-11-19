import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace",
        "re":"http://exslt.org/regular-expressions"}

def fol_to_page(fol:str):
    sub = 0 if fol[-1]=='v' else 1
    return int(fol[:-1])*2-sub


def file_to_page_am(fol:str):
    folgrp=re.match(r"(\d+[vr])-?(\d+[vr])?",fol).groups()
    if folgrp[1] is None:
        return 1
    return fol_to_page(folgrp[1]) - fol_to_page(folgrp[0])+1


def facs_list(file:str):
    """creates list of number of elements related to facssimile if there is a missmatch"""

    annot_li = []
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    filematch = re.match(r".+(n\d+)_([0-9rv-]+)_enc(_(ed|dipl)_(CMN|GLT))",file)

    pagenum=file_to_page_am(filematch.group(2))

    filename = "".join(filematch.groups()[0:3:2])

    surfacesnum = len(root.xpath(".//mei:surface", namespaces=ns))
    output = surfacesnum != pagenum
    if "enc_ed" in file:
        facsnum = len(root.xpath(".//mei:section[@n and @facs]", namespaces=ns))
        output = output or facsnum != pagenum
    elif "enc_dipl" in file:
        pbs = root.xpath(".//mei:pb[@n and @facs]", namespaces=ns)
        facsnum = len(pbs)
        choicepb=0
        for pb in pbs: #pbs might be in a choice and therefor doubled
            if pb.xpath("ancestor::mei:choice",namespaces=ns):
                choicepb+=1
        facsnum-=choicepb/2
        output = output or facsnum != pagenum
    candidates = root.xpath(".//mei:rend", namespaces=ns)
    foldirs=[el for el in candidates if re.match(r"fol\. \d+[rv]", el.text or "")]
    foldirsnum = len(foldirs)
    choicefol=0
    for foldir in foldirs: #foldirs might be in a choice and therefor doubled
        if foldir.xpath("ancestor::mei:choice",namespaces=ns):
            choicefol+=1
    foldirsnum-=choicefol/2
    output = output or foldirsnum != pagenum

    return [filename,str(pagenum),str(surfacesnum),str(facsnum),str(foldirsnum)] if output else[]
#if output else []


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    facs_matrix=[]
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(ed|dipl)_(CMN|GLT)\.mei",file)!=None:
                facs_li = facs_list(os.path.join(root,file))
                if facs_li:
                    facs_matrix.append("\t".join(facs_li))

    print("File\t\tpages\tsurface\t@facs\tfoldir")
    print("\n".join(facs_matrix))

choosefile()