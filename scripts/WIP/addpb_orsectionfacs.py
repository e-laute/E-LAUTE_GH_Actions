import copy
import os
import re
import sys

from lxml import etree

ns = {
    "mei": "http://www.music-encoding.org/ns/mei",
    "xml": "http://www.w3.org/XML/1998/namespace",
}


def change_lr(file: str):
    tree = etree.parse(file)

    root = tree.getroot()

    section = root.find(".//mei:section", namespaces=ns)

    surface = root.find(".//mei:surface", namespaces=ns)
    surf_id = surface.get("{http://www.w3.org/XML/1998/namespace}id")
    fol = re.match(r".*\\Jud_1523-2_n\d+_(\d+(v|r)).*", file).group(1)
    # if surface is not None:
    # if "enc_dipl_CMN" in file:
    # print(section.attrib)
    # if section.find(".//pb", ns) is None:
    # pb = ET.Element("pb",{"n":fol,"facs":f"#{surf_id}"})
    # section.insert(0,pb)
    # if "enc_ed_CMN" in file:
    # section.set("n",fol)
    # section.set("facs",f"#{surf_id}")
    measure = section.find(".//mei:measure", namespaces=ns)
    dir = etree.SubElement(
        measure, "dir", {"staff": "1", "tstamp": "1", "place": "above", "type": "ref"}
    )
    rend = etree.SubElement(
        dir, "rend", {"fontstyle": "normal", "fontsize": "xx-small"}
    )
    rend.text = f"fol. {fol}"

    etree.indent(tree, "   ")

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r"Jud.*_enc_dipl_CMN\.mei", file) != None:
                if re.match(r".*_n(1|2)[0-9]", file) is not None or "n25" in file:
                    continue
                # print(os.path.join(root,file))
                # if re.sub(r"(.*)_enc_.*_CMN(\.mei)",r"\1_enc_ed_GLT\2",file) in files:
                # print("\nsucces: ",os.path.join(root,re.sub(r"(.*)_enc_.*_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))
                change_lr(os.path.join(root, file))


choosefile()
