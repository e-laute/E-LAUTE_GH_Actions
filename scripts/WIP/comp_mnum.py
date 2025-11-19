import os
import re
from lxml import etree as ET
import copy
import sys

#TODO add measure elem comparision

ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace",
        "re":"http://exslt.org/regular-expressions"}


def getmnum(file:str):
    """returns @n of last measure"""

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    measures = root.xpath("//mei:measure", namespaces=ns)
    measures_invis = root.xpath("//mei:measure[@right='invis']", namespaces=ns)
    endings = root.xpath("//mei:ending", namespaces=ns)
    has_pickup = measures[0].get("type","no")=="pickup"
    
    return (measures[-1].get("n","no_n"), str(len(measures)-int(len(endings)/2)-has_pickup), str(len(measures)), str(len(measures_invis)))

def create_list(root:str,files:list):
    dipl_glt=("fnf",0)
    dipl_CMN=("fnf",0)
    ed_glt=("fnf",0)
    ed_CMN=("fnf",0)
    filename=("error",0)

    for file in files:
        filematch = re.match(r".+(n\d+)_([0-9rv-]+)_enc_((ed|dipl)_(CMN|GLT))\.mei",file)
        if filematch is not None:
            filename = filematch.group(1)
            match filematch.group(3):
                case "dipl_GLT":
                    dipl_glt = getmnum(os.path.join(root,file))[0:2]
                case "dipl_CMN":
                    dipl_CMN = getmnum(os.path.join(root,file))[0:2]
                case "ed_GLT":
                    ed_glt = getmnum(os.path.join(root,file))
                case "ed_CMN":
                    ed_CMN = getmnum(os.path.join(root,file))
    
    #return [filename,dipl_glt,dipl_CMN,ed_glt,ed_CMN] if not (dipl_glt[0]==dipl_CMN[0]==ed_glt[0]==ed_CMN[0] and dipl_glt[1]==dipl_CMN[1] and ed_glt[1]==ed_CMN[1] and ed_glt[2]==ed_CMN[2]=="0") else []
    return [filename,ed_glt] if not (ed_glt[0]==ed_glt[1] and ed_glt[3]=="0") else []

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    facs_matrix=[]
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(dipl)_(GLT)\.mei",file)!=None:
                facs_li = create_list(root,files)
                if facs_li:
                    facs_matrix.append("\t".join(["|".join(f) if isinstance(f,tuple) else f for f in facs_li ]))

    #print("File\tdi_GLT\tdi_CMN\ted_GLT\ted_CMN")
    print("File\ted_GLT")
    print("\n".join(facs_matrix))

choosefile()