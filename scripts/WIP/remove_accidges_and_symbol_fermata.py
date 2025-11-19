import xml.etree.ElementTree as ET
import copy
import sys
import os

def fixbeam(file:str):
    ns = {"":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}
    
    XML_ID="{http://www.w3.org/XML/1998/namespace}id"

    ET.register_namespace("","http://www.music-encoding.org/ns/mei")

    doc = ET.parse(file, parser = ET.XMLParser(encoding = 'utf-8'))

    root = doc.getroot()

    notes = root.findall(".//note", ns)

    for note in notes:
        if "accid.ges" in note.attrib and "accid" not in note.attrib:
            note.set("accid",note.get("accid.ges"))
            note.attrib.pop("accid.ges")

    fermatas = root.findall('.//dir/symbol[@glyph.name="fermataAbove"]/..', ns)
    for fermata in fermatas:
        id = fermata.get("{http://www.w3.org/XML/1998/namespace}id")
        startid = fermata.get("startid")
        measure = root.find(f'.//measure/dir[@{XML_ID}="{id}"]/..', ns)
        ET.SubElement(measure,"fermata",{XML_ID:id,"startid":f"{startid}"})
        measure.remove(fermata)
        

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
            if "enc_dipl_CMN.mei" in file and "converted" not in root:
                print(os.path.join(root,file))
                fixbeam(os.path.join(root,file))
                #os.rename(os.path.join(root, file),os.path.join(root, dirname + file[file.index("_enc_"):]))

changefilename()