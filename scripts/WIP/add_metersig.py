import copy
import os
import re
import sys

from lxml import etree

ns = {
    "mei": "http://www.music-encoding.org/ns/mei",
    "xml": "http://www.w3.org/XML/1998/namespace",
}


def update_ed_CMN_staffdef(file: str):
    tree = etree.parse(file)

    root = tree.getroot()

    meterSig = root.find(".//mei:meterSig", namespace=ns)

    mensur = root.find(".//mei:mensur", namespace=ns)

    if mensur is not None and meterSig is None:
        staffdefs = root.findall(".//mei:staffDef", namespace=ns)
        if len(staffdefs) < 2:
            print(f"not enough staffdef for {file}")
            return
        meterSig = etree.Element("meterSig")
        if mensur.get("num") is not None:
            meterSig.set("count", "3")
        elif mensur.get("slash") is not None and mensur.get("slash") == "1":
            meterSig.set("count", "4")
        else:
            meterSig.set("count", "2")
        meterSig.set("unit", "4")
        meterSig.set("enclose", "brack")

        staffdefs[0].append(meterSig)
        staffdefs[1].append(meterSig)

    etree.indent(tree, "   ")

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if re.fullmatch(r"Jud.*_enc_ed_CMN\.mei", file) != None:
                if re.match(r"_n1[0-6]", file) is not None or "n25" in file:
                    print(os.path.join(root, file))
                    continue
                # if re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file) in files:
                # print("\nsucces\n",os.path.join(root,re.sub(r"(.*)_enc_ed_CMN(\.mei)",r"\1_enc_ed_GLT\2",file)))
                update_ed_CMN_staffdef(os.path.join(root, file))


choosefile()
