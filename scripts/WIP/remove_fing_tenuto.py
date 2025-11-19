import xml.etree.ElementTree as ET
import copy
import sys
import os

def fixbeam(file:str):
    ns = {"":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

    ET.register_namespace("","http://www.music-encoding.org/ns/mei")

    doc = ET.parse(file, parser = ET.XMLParser(encoding = 'utf-8'))

    root = doc.getroot()

    measures = root.findall(".//measure", ns)

    
    staffdef = root.find(".//staffDef", ns)
    keysig = staffdef.find(".//keySig", ns)
    if keysig is not None:
        staffdef.remove(keysig)

    for measure in measures:
        fings=measure.findall(".//unclear", ns)
        annots = [
            annot for annot in measure.findall(".//annot",ns)
            if annot.text and annot.text.strip().endswith("cross")
        ]
        for annot in annots:
            try:
                measure.remove(annot)
            except ValueError:
                continue
        
        for fing in fings:
            if fing.find(".//fing",ns) is not None:
                try:
                    measure.remove(fing)
                except ValueError:
                    continue

    mei = ""
    with open(file, "r", encoding="utf-8") as f:
        while True:
            line = f.readline()
            if line[:2] != "<?":
                break
            mei += line
        ET.indent(root, "   ")
        mei += ET.tostring(root, "unicode")

    with open(file, "w", encoding="utf-8") as f:
        f.write(mei)

def changefilename():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    print(dir_path)

    for root, dirs, files in os.walk(dir_path):
        for file in files:
            dirname = root.split("/")[-1]
            if "enc_dipl_CMN.mei" in file:
                print(os.path.join(root,file))
                fixbeam(os.path.join(root,file))
                #os.rename(os.path.join(root, file),os.path.join(root, dirname + file[file.index("_enc_"):]))

changefilename()