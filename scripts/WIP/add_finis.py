import copy
import os
import re
import sys

from lxml import etree
from utils import *

ns = {
    "mei": "http://www.music-encoding.org/ns/mei",
    "xml": "http://www.w3.org/XML/1998/namespace",
}


def guess_string_width(text):
    if not text:
        return 0

    # Split by lines and find the longest one (visual width-wise)
    lines = text.splitlines()

    # Character weight map based on a typical sans-serif font
    # Normalized so that a standard lowercase letter or space is ~1.0
    weights = {
        "wide": "MWm@W",  # Weight: 1.5
        "narrow": "ilj|!:.tI[]",  # Weight: 0.4
        "caps": "ABCDEFGHJKLNPQRSTUVXYZ",  # Weight: 1.2
    }

    def get_line_width(line):
        width = 0.0
        for char in line:
            if char in weights["wide"]:
                width += 1.5
            elif char in weights["narrow"]:
                width += 0.4
            elif char.isupper():
                width += 1.2
            elif char == "\t":
                width += 4.0  # Standard 4-space tab
            else:
                width += 1.0  # Default for lowercase, numbers, and spaces
        return width

    return max(get_line_width(line) for line in lines)


def add_finis(file: str):
    """adds finis on the last tstamp+1 of the last measure, doesnt account for mark-up or n-tuplets"""
    with open(file, "rb") as f:
        tree = etree.parse(f, etree.XMLParser(recover=True))
    root = tree.getroot()

    meterSig = root.find(".//mei:meterSig", namespaces=ns)

    # remove before adding again
    finis = root.xpath("//mei:dir[@type='finis']", namespaces=ns)
    if not finis:
        return
        measure = root.xpath("//mei:measure", namespaces=ns)[-1]
        layer = measure.find(".//mei:layer", namespaces=ns)
        tstamp = measure_length(layer) * int(meterSig.get("unit", "4"))
        dir = etree.SubElement(
            measure,
            "dir",
            {"staff": "2", "tstamp": str(tstamp), "place": "above", "type": "finis"},
        )
        dir.text = "Finis"
        print(f"added finis to {file}")

    for fin in finis:
        measure = fin.xpath("(ancestor::mei:measure)[1]", namespaces=ns)[0]
        layer = measure.find(".//mei:layer", namespaces=ns)
        tstamp = dur_length(layer)
        if meterSig is not None:
            tstamp = tstamp * int(meterSig.get("unit", "4"))
        else:
            tstamp *= 4
            for _ in range(10):
                if tstamp.is_integer():
                    break
                tstamp *= 2
            else:
                print(f"{fin.attrib} has problematic tstamp calculation")
                return
        fin.set("tstamp", str(tstamp + 1))
        vu = str(
            round(guess_string_width(fin.text) * 2.5) + 5
        )  # vu to whitespace ca. 2.5 for wordlength + 5 as spacing
        fin.set("startho", f"{vu}vu")

    etree.register_namespace("mei", ns["mei"])
    etree.register_namespace("xml", ns["xml"])
    etree.indent(tree, "   ")

    # change processing instructiuns and version to mei 5.1

    root.set("meiversion", "5.1")
    for pi in tree.xpath("//processing-instruction()"):
        if pi.target == "xml-model":
            pi.text = re.sub(
                r'href="[^"]+"',
                'href="https://music-encoding.org/schema/5.1/mei-all.rng"',
                pi.text,
            )

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.*_enc_(ed|dipl)_(CMN|GLT)\.mei", file) != None:
                add_finis(os.path.join(root, file))


choosefile()
