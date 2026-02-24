import copy
import os
import re
import sys
import utils

from lxml import etree

# TODO clean up, find reliable system

ns = {
    "mei": "http://www.music-encoding.org/ns/mei",
    "xml": "http://www.w3.org/XML/1998/namespace",
}


def getmnum(root: etree.Element):
    """returns @n of last measure"""

    measures = root.xpath("//mei:measure", namespaces=ns)

    return int(measures[-1].get("n", "no_n"))


def add_foldir(measure: etree.Element, fol: str, tstamp: str):
    dir = etree.SubElement(
        measure,
        "dir",
        {"staff": "1", "tstamp": tstamp, "place": "above", "type": "ref"},
    )
    rend = etree.SubElement(dir, "rend", {"fontstyle": "normal", "fontsize": "x-small"})
    rend.text = f"fol. {fol}"


def manual_unwrap(element):
    parent = element.getparent()
    if parent is None:
        return  # Cannot unwrap the root

    # Get the index of the element to maintain position
    index = parent.index(element)

    # Move children and handle text/tails
    previous = element.getprevious()
    if previous is not None:
        previous.tail = (previous.tail or "") + (element.text or "")
    else:
        parent.text = (parent.text or "") + (element.text or "")

    # Reverse iterate to keep indices stable during insertion
    for child in reversed(element):
        parent.insert(index, child)

    # Finally, remove the tag
    parent.remove(element)


def add_section_foldir_to_ed(active_dom: dict, context_doms: list, **addargs):
    """
    Removes expansion and section containing sections, adds foldir and sections/pbs based on dipl or ed

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    output_message = ""

    if "ed" not in active_dom["filetype"]:
        raise RuntimeError(f"{active_dom['filename']} must be ed_GLT or ed_CMN")

    root = active_dom["dom"]
    root: etree.Element = active_dom["dom"]
    for context_dom in context_doms:
        if context_dom["notationtype"] == "dipl_GLT":
            help_dom = context_dom
            helproot = help_dom["dom"]
            break
    else:
        raise RuntimeError(
            "add_section_foldir_from_dipl_GLT_to_ed needs context_dom dipl_GLT, not given"
        )

    if "ed" in active_dom["filetype"]:
        section_in_section = root.xpath(".//mei:section//mei:section", namespaces=ns)

        expansion = root.find(".//mei:expansion", namespaces=ns)
        if expansion is not None:
            expansion.getparent().remove(expansion)
            for s in section_in_section:
                manual_unwrap(s)
        elif section_in_section:
            RuntimeError(
                f"{active_dom["filename"]} contains recursive section but no expansion"
            )

    section = root.find(".//mei:section", namespaces=ns)

    if "dipl" in help_dom["notationtype"]:
        section_info = get_section_info_dipl(helproot, help_dom)
    else:
        if getmnum(root) == getmnum(helproot):
            section_info = get_section_info_ed(helproot, help_dom)
        else:
            raise RuntimeError(
                f"{active_dom["filename"]} has diffrent number of measures from {help_dom["filename"]}"
            )

    surfaces = root.xpath("//mei:facsimile/mei:surface", namespaces=ns)
    if len(surfaces) != len(section_info):
        raise RuntimeError(
            f"surfaces don't match number of sections in {active_dom["filename"]}"
        )

    surface_id = [f"#{s.get(f'{{{ns['xml']}}}id')}" for s in surfaces]

    score = section.getparent()

    section_info = [
        (
            mnum,
            fol,
            tstamp,
            etree.SubElement(score, "section", {"n": fol, "facs": facs}),
        )
        for (mnum, fol, tstamp), facs in zip(section_info, surface_id)
    ]

    section_children = list(section)
    current_n = ""
    current_section = 0
    for child in section_children:
        if child.tag == f"{{{ns['mei']}}}measure":
            current_n = child.get("n", "")
            if (
                current_section != len(section_info) - 1
                and current_n == section_info[current_section + 1][0]
            ):
                current_section += 1
                section_info[current_section][3].append(child)
                add_foldir(
                    child,
                    section_info[current_section][1],
                    section_info[current_section][2],
                )
            else:
                section_info[current_section][3].append(child)
        else:
            section_info[current_section][3].append(child)

    if current_section != len(section_info) - 1:
        raise RuntimeError(
            f"{active_dom["filename"]} wasn't processed because of misalignment of section"
        )

    print("succes!!")

    score.remove(section)

    active_dom["dom"] = root
    return active_dom, output_message


def get_section_info_dipl(help_dom: dict):
    pb_measures = help_dom["dom"].xpath("./mei:dir[@type='ref']/..", namespaces=ns)

    section_info = []

    for pb_measure in pb_measures:
        if pb_measure.tag != f"{{{ns['mei']}}}measure":
            raise RuntimeError(
                f"foldir parent wasn't measure in {help_dom["filename"]} at {pb_measure.get("n","no_n_found")}"
            )
        if pb_measure.xpath("ancestor::mei:orig", namespaces=ns):
            continue
        if pb_measure.xpath("ancestor::mei:reg", namespaces=ns):
            pbs = pb_measure.getparent().xpath("./mei:pb", namespaces=ns)
            if len(pbs) != 1:
                raise RuntimeError(
                    f"More than one pb in reg around {pb_measure.get("n","no_n_found")}"
                )
            pb = pbs[0]
        else:
            pb = get_previous_at_same_depth(help_dom["dom"], pb_measure)
            if pb is None or pb.tag != f"{{{ns['mei']}}}pb":
                raise RuntimeError(
                    f"No pb found before {pb_measure.get("n","no_n_found")}"
                )
        foldir = pb_measure.find("./mei:dir[@type='ref']", namespaces=ns)
        tstamp = foldir.get("tstamp")
        section_info.append((pb_measure.get("n"), pb.get("n"), tstamp))
    return section_info


def get_section_info_ed(help_dom: dict):
    help_sections = help_dom["dom"].xpath("//mei:section[@n]", namespaces=ns)

    section_info = []
    for help_section in help_sections:
        help_measures = help_section.xpath(".//mei:measure", namespaces=ns)
        foldir = help_measures[0].xpath("./mei:dir[@type='ref']", namespaces=ns)
        if not foldir:
            raise RuntimeError(f"no foldir found in {help_dom["filename"]}")
        tstamp = foldir[0].get("tstamp")
        section_info.append((help_measures[0].get("n"), help_section.get("n"), tstamp))

    return section_info


def get_previous_at_same_depth(tree, element):
    depth = utils.get_depth(element)
    same_depth = [el for el in tree.iter() if utils.get_depth(el) == depth]
    idx = same_depth.index(element)
    return same_depth[idx - 1] if idx > 0 else None


def add_facs_and_foldir(file: str, helpfile: str):
    with open(file, "rb") as h:
        tree = etree.parse(h, etree.XMLParser(recover=True))

    with open(helpfile, "rb") as h:
        helptree = etree.parse(h, etree.XMLParser(recover=True))
    helproot = helptree.getroot()

    root = tree.getroot()

    section = root.xpath(".//mei:section//mei:section", namespaces=ns)

    expansion = root.find(".//mei:expansion", namespaces=ns)

    if expansion is not None:
        expansion.getparent().remove(expansion)
        for s in section:
            manual_unwrap(s)
    elif section:
        print(f"{file} contains recursive section but no expansion")
        return

    etree.indent(tree, "   ")

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

    # surface = root.find(".//mei:surface", namespaces=ns)
    # surf_id = surface.get("{http://www.w3.org/XML/1998/namespace}id")
    # fol = re.match(r".*\\A-Wn.*_n\d+_(\d+(v|r)).*", file).group(1)
    # if surface is not None:
    # if "enc_dipl_CMN" in file:
    # print(section.attrib)
    # if section.find(".//pb", ns) is None:
    # pb = ET.Element("pb",{"n":fol,"facs":f"#{surf_id}"})
    # section.insert(0,pb)
    # if "enc_ed_CMN" in file:
    #   section.set("n", fol)
    #  section.set("facs", f"#{surf_id}")
    """
    measure = section.find(".//mei:measure", namespaces=ns)
    """

    section = root.find(".//mei:section", namespaces=ns)

    if getmnum(root) == getmnum(helproot):
        help_sections = helproot.xpath("//mei:section[@n]", namespaces=ns)
    else:
        print(f"{file} has diffrent number of measures from ed_GLT")
        return

    section_info = []
    for help_section in help_sections:
        help_measures = help_section.xpath(".//mei:measure", namespaces=ns)
        foldir = help_measures[0].xpath("./mei:dir[@type='ref']", namespaces=ns)
        if not foldir:
            print(f"no foldir found in ed_GLT of {file}")
            return
        tstamp = foldir[0].get("tstamp")
        section_info.append((help_measures[0].get("n"), help_section.get("n"), tstamp))

    surfaces = root.xpath("//mei:facsimile/mei:surface", namespaces=ns)
    if len(surfaces) != len(section_info):
        print(f"surfaces don't match number of sections in {file}")
        return

    surface_id = [f"#{s.get(f'{{{ns['xml']}}}id')}" for s in surfaces]

    score = section.getparent()

    section_info = [
        (
            mnum,
            fol,
            tstamp,
            etree.SubElement(score, "section", {"n": fol, "facs": facs}),
        )
        for (mnum, fol, tstamp), facs in zip(section_info, surface_id)
    ]

    section_children = list(section)
    current_n = ""
    current_section = 0
    for child in section_children:
        if child.tag == f"{{{ns['mei']}}}measure":
            current_n = child.get("n", "")
            if (
                current_section != len(section_info) - 1
                and current_n == section_info[current_section + 1][0]
            ):
                current_section += 1
                section_info[current_section][3].append(child)
                add_foldir(
                    child,
                    section_info[current_section][1],
                    section_info[current_section][2],
                )
            else:
                section_info[current_section][3].append(child)
        else:
            section_info[current_section][3].append(child)

    if current_section != len(section_info) - 1:
        print(f"{file} wasn't processed because of misalignment of section")
        return

    print("succes!!")

    score.remove(section)

    etree.indent(tree, "   ")

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if (
                re.fullmatch(r"A-Wn.*n(?!0[1-5])\d{2}.*_enc_ed_CMN\.mei", file)
                is not None
            ):
                try:
                    add_facs_and_foldir(
                        os.path.join(root, file),
                        os.path.join(
                            root,
                            re.sub(r"(.*)_enc_.*_CMN(\.mei)", r"\1_enc_ed_GLT\2", file),
                        ),
                    )
                except Exception as e:
                    print(e)


choosefile()
