"""
Derive alternate tablature notation types from currently GLT.mei files.


Usage:
python scripts/derive-alternate-tablature-notation-types.py <folder>

where <folder> is a relative or absolute path to process.

The script will:
1. Process the GLT.mei files in the given folder and subfolders.
2. Create FLT and ILT files from the GLT.mei files.

TODO: make this tablature type acnostic and make sure that it works depending on type of input tablature

"""

import argparse
import os
import random
import shutil
import string
import xml.etree.ElementTree as ET
from datetime import date

VERSION = "1.0"  # Version of this conversion script

GLT_to_F_and_ILT = ["enc_dipl_GLT.mei", "enc_ed_GLT.mei"]

# XML declaration and model declarations
XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8"?>'
RELAXNG_MODEL = '<?xml-model href="https://music-encoding.org/schema/5.1/mei-all.rng" type="application/xml" schematypens="http://relaxng.org/ns/structure/1.0"?>'
SCHEMATRON_MODEL = '<?xml-model href="https://music-encoding.org/schema/5.1/mei-all.rng" type="application/xml" schematypens="http://purl.oclc.org/dsdl/schematron"?>'

ET.register_namespace("", "http://www.music-encoding.org/ns/mei")


# Inject XML file with proper model declarations
def inject_xml_model_declaration(tree, file_name):
    # Indent the tree for proper formatting
    ET.indent(tree, space="  ", level=0)

    # Get the XML content as string without the declaration
    xml_content = ET.tostring(tree.getroot(), encoding="unicode")

    # Build the complete file content with declarations
    content_lines = [
        XML_DECLARATION,
        RELAXNG_MODEL,
        SCHEMATRON_MODEL,
        xml_content,
    ]

    with open(file_name, "w", encoding="utf-8") as file:
        file.write("\n".join(content_lines))


# TODO: move to utils.py (coordinate with Henning)
def ensure_application_info(
    tree_root, input_filename, output_filename, version, app_name
):
    ns = "{http://www.music-encoding.org/ns/mei}"
    app_info = tree_root.find(f".//{ns}appInfo")
    if app_info is None:
        encoding_desc = tree_root.find(f".//{ns}encodingDesc")
        if encoding_desc is None:
            mei_head = tree_root.find(f".//{ns}meiHead")
            if mei_head is None:
                mei_head = ET.SubElement(tree_root, f"{ns}meiHead")
            encoding_desc = ET.SubElement(mei_head, f"{ns}encodingDesc")
        app_info = ET.SubElement(encoding_desc, f"{ns}appInfo")

    for existing_app in list(app_info.findall(f"{ns}application")):
        name_node = existing_app.find(f"{ns}name")
        if name_node is None:
            continue
        name_text = (
            name_node.text.strip() if isinstance(name_node.text, str) else ""
        )
        if name_text == app_name:
            app_info.remove(existing_app)

    # Generate xml:id: letter + 7 alphanumeric characters
    random_id = random.choice(string.ascii_lowercase) + "".join(
        random.choices(string.ascii_lowercase + string.digits, k=7)
    )

    application = ET.SubElement(
        app_info,
        f"{ns}application",
        {
            "isodate": date.today().isoformat(),
            "version": version,
            "{http://www.w3.org/XML/1998/namespace}id": random_id,
        },
    )
    app_name_node = ET.SubElement(application, f"{ns}name")
    app_name_node.text = app_name

    # Add conversion information
    p_from = ET.SubElement(application, f"{ns}p")
    p_from.text = f"Input file: {input_filename}"

    p_to = ET.SubElement(application, f"{ns}p")
    p_to.text = f"Output file: {output_filename}"


# Process MEI files to convert GLT to FLT and ILT
def process_mei_file(input_file, output_dir):
    french_dir = os.path.join(output_dir, "FLT")
    italian_dir = os.path.join(output_dir, "ILT")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(french_dir, exist_ok=True)
    os.makedirs(italian_dir, exist_ok=True)
    if not input_file.endswith("GLT.mei"):
        return False
    try:
        tree = ET.parse(input_file)
        tree_root = tree.getroot()

        output_french = os.path.join(
            french_dir, os.path.basename(input_file.replace("GLT", "FLT"))
        )
        output_italian = os.path.join(
            italian_dir, os.path.basename(input_file.replace("GLT", "ILT"))
        )

        # Remove rests from the tree
        def iterator(parents):
            for child in reversed(parents):
                if len(child) >= 1:
                    iterator(child)
                if child.tag == "{http://www.music-encoding.org/ns/mei}rest":
                    if (
                        parents.tag
                        == "{http://www.music-encoding.org/ns/mei}tabGrp"
                    ):
                        has_tabDurSym = any(
                            c.tag
                            == "{http://www.music-encoding.org/ns/mei}tabDurSym"
                            for c in parents
                        )
                        if not has_tabDurSym:
                            parents.append(
                                ET.Element(
                                    "{http://www.music-encoding.org/ns/mei}tabDurSym",
                                )
                            )
                    parents.remove(child)

        iterator(tree_root)

        # Remove tab.line from <note> and <tabDurSym>
        for note in tree_root.findall(
            ".//{http://www.music-encoding.org/ns/mei}note"
        ):
            if "tab.line" in note.attrib:
                note.attrib.pop("tab.line")
        for tabDurSym in tree_root.findall(
            ".//{http://www.music-encoding.org/ns/mei}tabDurSym"
        ):
            if "tab.line" in tabDurSym.attrib:
                tabDurSym.attrib.pop("tab.line")

        staffDef = tree_root.find(
            ".//{http://www.music-encoding.org/ns/mei}staffDef"
        )
        title_part_abbr = tree_root.find(
            ".//{http://www.music-encoding.org/ns/mei}title/{http://www.music-encoding.org/ns/mei}titlePart/{http://www.music-encoding.org/ns/mei}abbr"
        )
        # Ensure, that lines='6' and remove tab.align and tab.anchorline
        staffDef.set("lines", "6")
        staffDef.attrib.pop("tab.align", None)
        staffDef.attrib.pop("tab.anchorline", None)

        # write French file
        staffDef.set("notationtype", "tab.lute.french")
        if title_part_abbr is not None:
            title_part_abbr.text = "FLT"
            title_part_abbr.set("expan", "French Lute Tablature")
        ensure_application_info(
            tree_root,
            os.path.basename(input_file),
            os.path.basename(output_french),
            VERSION,
            os.path.basename(__file__),
        )
        inject_xml_model_declaration(tree, output_french)

        # write Italian file
        staffDef.set("notationtype", "tab.lute.italian")
        if title_part_abbr is not None:
            title_part_abbr.text = "ILT"
            title_part_abbr.set("expan", "Italian Lute Tablature")
        ensure_application_info(
            tree_root,
            os.path.basename(input_file),
            os.path.basename(output_italian),
            VERSION,
            os.path.basename(__file__),
        )
        inject_xml_model_declaration(tree, output_italian)
        return True
    except Exception:
        return False


def _resolve_output_path_with_collision_suffix(
    target_dir, source_file_path, seen_targets
):
    """Create a deterministic output path and avoid name collisions."""
    base_name = os.path.basename(source_file_path)
    stem, ext = os.path.splitext(base_name)
    candidate_path = os.path.join(target_dir, base_name)
    counter = 1
    while candidate_path in seen_targets or os.path.exists(candidate_path):
        candidate_name = f"{stem}__dup{counter}{ext}"
        candidate_path = os.path.join(target_dir, candidate_name)
        counter += 1
    seen_targets.add(candidate_path)
    return candidate_path


def process_directory_recursively(folder_path):
    """Process all MEI files recursively and save into one top-level converted folder."""
    output_dir = os.path.join(folder_path, "converted")
    german_dir = os.path.join(output_dir, "GLT")
    french_dir = os.path.join(output_dir, "FLT")
    italian_dir = os.path.join(output_dir, "ILT")
    cmn_dir = os.path.join(output_dir, "CMN")

    for directory in (german_dir, french_dir, italian_dir, cmn_dir):
        os.makedirs(directory, exist_ok=True)

    seen_glt_targets = set()
    seen_cmn_targets = set()

    for root, dirs, files in os.walk(folder_path):
        # Never recurse into the generated output folder.
        if "converted" in dirs:
            dirs.remove("converted")

        for file in files:
            file_path = os.path.join(root, file)

            if file.endswith("GLT.mei"):
                if process_mei_file(file_path, output_dir):
                    glt_target = _resolve_output_path_with_collision_suffix(
                        german_dir, file_path, seen_glt_targets
                    )
                    shutil.copy(file_path, glt_target)
                else:
                    continue

            elif file.endswith("CMN.mei"):
                try:
                    cmn_target = _resolve_output_path_with_collision_suffix(
                        cmn_dir, file_path, seen_cmn_targets
                    )
                    shutil.copy(file_path, cmn_target)
                except Exception:
                    continue


def main():
    """
    Use as follows (locally in .venv):
    python scripts/derive-alternate-tablature-notation-types.py <folder>

    where <folder> is a relative or absolute path to process.
    """

    parser = argparse.ArgumentParser(
        description="Convert GLT MEI files to FLT and ILT variants."
    )
    parser.add_argument(
        "folder",
        help="Folder containing MEI files to convert (relative or absolute).",
    )
    args = parser.parse_args()

    # Resolve the target path
    target_path = args.folder
    if not os.path.isabs(target_path):
        # Relative paths are resolved from the current working directory
        target_path = os.path.abspath(target_path)

    if not os.path.isdir(target_path):
        return

    process_directory_recursively(target_path)


if __name__ == "__main__":
    main()
