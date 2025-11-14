"""
Ensure that all <corr> elements in the given MEI file have a 'cert' attribute. Requires one argument: path to the MEI file.
"""
import os
import re
from lxml import etree as ET
import copy
import sys
from pathlib import Path


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}



def ensure_cert(file:str):
    """ensure corr has cert"""
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()
    try: 
        #remove before adding again
        corrs = root.xpath("//mei:choice/mei:corr", namespaces=ns)

        for corr in corrs:
            if corr.get("cert") is None:
                corr.set("cert","medium")


        ET.register_namespace("mei", ns["mei"])
        ET.register_namespace("xml", ns["xml"])
        ET.indent(tree,"   ")

        #change processing instructiuns and version to mei 5.1
            

        # Write back, preserving XML declaration and processing instructions
        with open(file, "wb") as f:
            tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)
    except Exception as e:
        print(f"Error processing file {file}: {e}")

def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(ed|dipl)_(CMN|GLT)\.mei",file)!=None:
                ensure_cert(os.path.join(root,file))

#choosefile()

def main(argv: list[str]):
    if len(argv) != 2 or argv[0] in {"-h", "--help"}:
        print(__doc__.strip())
        print("Received inputs:", argv)
        return 1

    mei_path = Path(argv[0])
    if not mei_path.is_file():
        print(f"Error: '{mei_path}' is not a file.", file=sys.stderr)
        return 2
    try:
        ensure_cert(mei_path)
        return 0
    except Exception as e:
        print(f"Error processing file: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv)) 