import os
import re
import xml.etree.ElementTree as ET
import copy
import sys

#TODO to lxml, mit add facs

ns = {"":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

def measure_lr(measure:ET.Element,left:bool,skip=False):
    print(measure.attrib)
    ret = "left" in measure.attrib and measure.attrib["left"]=="invis"
    if skip:
        annot = ET.SubElement(measure,"{http://www.music-encoding.org/ns/mei}annot",{"plist":f"#{measure.get("{http://www.w3.org/XML/1998/namespace}id")}"})
        annot.text="previous measures in choice differed in @left"
        measure.attrib.pop("left",None)
        return ret
    if left:
        if ("right" in measure.attrib and measure.attrib["right"]!="invis"):
            annot = ET.SubElement(measure,"{http://www.music-encoding.org/ns/mei}annot",{"plist":f"#{measure.get("{http://www.w3.org/XML/1998/namespace}id")}"})
            annot.text="right should be invis, had other value"
        else:
            measure.set("right","invis")
    measure.attrib.pop("left",None)
    return ret

def handle_choice(choice:ET.Element,left:bool):
    left_orig = left_reg = left
    try:
        for elem in choice.find("orig",ns)[::-1]:
                if elem.tag=="{http://www.music-encoding.org/ns/mei}measure":
                    left_orig=measure_lr(elem,left_orig)
        for elem in choice.find("reg",ns)[::-1]:
                if elem.tag=="{http://www.music-encoding.org/ns/mei}measure":
                    left_reg=measure_lr(elem,left_reg)
    except TypeError:
        return left
    print(left_orig,left_reg)
    if left_reg==left_orig:
        return left_reg
    return None
    



def change_lr(file:str,helpfile:str):

    ET.register_namespace("","http://www.music-encoding.org/ns/mei")

    doc = ET.parse(file)

    root = doc.getroot()

    helpdoc=ET.parse(helpfile)

    helproot = helpdoc.getroot()

    grpSym = root.find(".//grpSym", ns)

    grpSym.set("symbol","brace")

    meterSigs = root.findall(".//meterSig", ns)

    for meterSig in meterSigs:
        meterSig.set("enclose","brack")

    mensur = helproot.find(".//mensur", ns)

    if mensur is not None:
        print(mensur)
        newmensur = copy.deepcopy(mensur)
        newmensur.attrib.pop("{http://www.w3.org/XML/1998/namespace}:id",None)
        staffdefs = root.findall(".//staffDef", ns)
        if len(staffdefs) < 2:
            print(f"not enough staffdef for {file}")
            return
        staffdefs[0].append(newmensur)
        staffdefs[1].append(newmensur)

    mei = ""
    with open(file, "r", encoding="utf-8") as f:
        while True:
            line = f.readline()
            if line[:2] != "<?":
                break
            mei += line
        ET.indent(root, "   ")
        mei += ET.tostring(root, "unicode")
    """
    left=False
    skip_next=False
    for section in sections[::-1]:
        for elem in section[::-1]:
            print(elem.tag)
            if elem.tag=="{http://www.music-encoding.org/ns/mei}measure":
                left=measure_lr(elem,left,skip_next)
                skip_next=False
            if elem.tag=="{http://www.music-encoding.org/ns/mei}choice":
                left=handle_choice(elem,left)
                skip_next=left==None
            
        
"""

    with open(file, "w", encoding="utf-8") as f:
        f.write(mei)

def changefilename():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei",file)!=None:
                if re.match(r"_n1[0-6]",file) is not None or "n25" in file:
                    print(os.path.join(root,file))
                    continue
                #if re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file) in files:
                    #print("\nsucces\n",os.path.join(root,re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))
                change_lr(os.path.join(root,file),os.path.join(root,re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))

changefilename()