import copy
import math
import os
import re
import sys
from pathlib import Path

from lxml import etree
from utils import *

ns = {
    "mei": "http://www.music-encoding.org/ns/mei",
    "xml": "http://www.w3.org/XML/1998/namespace",
}

XML_ID = "{http://www.w3.org/XML/1998/namespace}id"


def add_sbs_every_n(active_dom: dict, context_doms: list, sbInterval: int, **addargs):
    """
    Adds `<sb>` every n measures

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param n: interval of system beginnings
    :type n: int
    :param addargs: Addional arguments that are unused
    """

    root = active_dom["dom"]

    measures = root.xpath(".//mei:measure", namespaces=ns)

    for count, measure in enumerate(measures):
        if (count + 1) % sbInterval == 0:
            sb = etree.Element("sb")
            parent = measure.getparent()
            parent.insert(parent.index(measure) + 1, sb)

    active_dom["dom"] = root
    output_message = ""
    return active_dom, output_message


def remove_all_sbs(active_dom: dict, context_doms: list, **addargs):
    """
    Removes all `<sb>`

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """

    root = active_dom["dom"]

    sbs = root.xpath(".//mei:sb", namespaces=ns)

    for sb in sbs:
        parent = sb.getparent()
        parent.remove(sb)

    active_dom["dom"] = root
    output_message = ""
    return active_dom, output_message


def _template_function(active_dom: dict, context_doms: list, **addargs):
    """
    template function

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    output_message = ""

    root = active_dom["dom"]

    xpath_result = root.xpath(".//mei:elem[@attrib='value']", namespaces=ns)

    active_dom["dom"] = root
    return active_dom, output_message


def compare_mnums(active_dom: dict, context_doms: list, **addargs):
    """
    Ouputs number measures and last @n across active and context doms

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """

    dipl_glt = "fnf"
    dipl_CMN = "fnf"
    ed_glt = "fnf"
    ed_CMN = "fnf"

    doms = [active_dom] + context_doms

    for dom in doms:
        match dom["notationtype"]:
            case "dipl_GLT":
                dipl_glt = getmnum(dom["dom"])[:2]
            case "dipl_CMN":
                dipl_CMN = getmnum(dom["dom"])[:2]
            case "ed_GLT":
                ed_glt = getmnum(dom["dom"])
            case "ed_CMN":
                ed_CMN = getmnum(dom["dom"])

    id_match = re.match(
        r".+(n\d+)_([0-9rv-]+)_enc_((ed|dipl)_(CMN|GLT))", active_dom["filename"]
    )
    if id_match:
        id_name = id_match.group(1)
    else:
        id_name = active_dom["filename"]
    output_list = [id_name, dipl_glt, dipl_CMN, ed_glt, ed_CMN]
    explainer = f"""The table shows all notationtypes found in the directory of {id_name} or fnf for (file not found)
The individual cells show the @n of the last measure, the number of measure elements and a hereustic for measure number.
"""
    content = "\t".join(
        ["|".join(f) if isinstance(f, tuple) else f for f in output_list]
    )
    output_message = explainer + "File\tdi_GLT\tdi_CMN\ted_GLT\ted_CMN\n" + content

    write_to_github_summary(content + "\n")

    return active_dom, output_message


def getmnum(root: etree.Element):
    """returns @n of last measure"""

    measures = root.xpath("//mei:measure", namespaces=ns)
    measures_invis = root.xpath("//mei:measure[@right='invis']", namespaces=ns)
    endings = root.xpath("//mei:ending", namespaces=ns)
    has_pickup = measures[0].get("type", "no") == "pickup"

    return (
        measures[-1].get("n", "no_n"),
        str(len(measures)),
        str(len(measures) - int(len(endings) / 2) - has_pickup - len(measures_invis)),
    )


def add_header_from_dipl_GLT(
    active_dom: dict, context_doms: list, projectstaff: str, role: str, **addargs
):
    """
    Adds header from dipl_GLT to ed_GLT, dipl_CMN or ed_CMN

    :param active_dom: dict containing {filename:str, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    # TODO needs to be adjusted

    output_message = ""

    root: etree.Element = active_dom["dom"]
    for context_dom in context_doms:
        if context_dom["notationtype"] == "dipl_GLT":
            helproot = context_dom["dom"]
            break
    else:
        raise RuntimeError("add_header_from_GLT needs context_dom dipl_GLT, not given")

    if root.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        raise RuntimeError(f"{active_dom["filename"]} already has E-Laute header")

    if not helproot.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        raise RuntimeError(f"{active_dom["filename"]} has no header")

    appInfo: etree.Element = root.find(".//mei:appInfo", namespaces=ns)

    help_header: etree.Element = copy.deepcopy(
        helproot.find(".//mei:meiHead", namespaces=ns)
    )

    abbr = help_header.find(".//mei:titlePart/mei:abbr", namespaces=ns)
    if "ed" in active_dom["notationtype"]:
        abbr.getparent().text = "edition in "
    if "CMN" in active_dom["notationtype"]:
        abbr.clear()
        abbr.set("expan", "Common Music Notation")
        abbr.text = "CMN"

    edition = help_header.find(".//mei:edition", namespaces=ns)
    edition.set("resp", f"#{projectstaff}")
    edition.text = f"First {'diplomatic transcription' if 'dipl' in active_dom["notationtype"] else 'edition'} in CMN. Lute tuned in A."

    appinfoold = help_header.find(".//mei:appInfo", namespaces=ns)
    encodingDesc = appinfoold.getparent()
    encodingDesc.remove(appinfoold)
    encodingDesc.insert(0, appInfo)

    revisionDesc = help_header.find("./mei:revisionDesc", namespaces=ns)
    del revisionDesc[1:]
    revisionDesc[0].attrib.update({"isodate": "YYYY-MM-DD", "n": "1", "resp": "#"})
    revisionDesc_ps = revisionDesc.xpath(".//mei:p", namespaces=ns)

    for revp in revisionDesc_ps:
        revp.text = ""

    root.remove(root.find("./mei:meiHead", namespaces=ns))
    root.insert(0, help_header)

    active_dom["dom"] = root
    return active_dom, output_message


def get_last_mnum(root: etree.Element):
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


def add_section_foldir_to_ed(
    active_dom: dict, context_doms: list, getPbFrom: str, **addargs
):
    """
    Removes expansion and section containing sections, adds foldir and sections/pbs based on dipl or ed

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    output_message = ""

    if "ed" not in active_dom["notationtype"]:
        raise RuntimeError(f"{active_dom['filename']} must be ed_GLT or ed_CMN")

    root = active_dom["dom"]
    root: etree.Element = active_dom["dom"]
    for context_dom in context_doms:
        if context_dom["notationtype"] == getPbFrom:
            help_dom = context_dom
            helproot = help_dom["dom"]
            break
    else:
        raise RuntimeError(
            f"add_section_foldir_from_dipl_GLT_to_ed needs context_dom {get_pb_from}, not found"
        )

    if "ed" in active_dom["notationtype"]:
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
        section_info = get_section_info_dipl(help_dom)
    else:
        if get_last_mnum(root) == get_last_mnum(helproot):
            section_info = get_section_info_ed(help_dom)
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
    print(section_info)
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
    depth = get_depth(element)
    same_depth = [el for el in tree.iter() if get_depth(el) == depth]
    idx = same_depth.index(element)
    return same_depth[idx - 1] if idx > 0 else None
