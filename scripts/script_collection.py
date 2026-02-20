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
    explainer = f"""The table shows all filetypes found in the directory of {id_name} or fnf for (file not found)
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
    Adds header from dipl_GLT

    :param active_dom: dict containing {filename:str, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    # TODO needs to be adjusted

    output_message = ""

    root = active_dom["dom"]
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

    appInfo = root.find(".//mei:appInfo", namespaces=ns)

    help_header = copy.deepcopy(helproot.find(".//mei:meiHead", namespaces=ns))

    abbr = help_header.find(".//mei:titlePart/mei:abbr", namespaces=ns)
    if "ed" in active_dom["notationtype"]:
        abbr.get_parent().text = "edition in "
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
