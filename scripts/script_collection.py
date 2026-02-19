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
    return active_dom


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
    return active_dom


def _template_function(active_dom: dict, context_doms: list, **addargs):
    """
    template function

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """

    root = active_dom["dom"]

    xpath_result = root.xpath(".//mei:elem[@attrib='value']", namespaces=ns)

    active_dom["dom"] = root
    return active_dom


def compare_mnums(active_dom: dict, context_doms: list, **addargs):
    """
    template function

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
                dipl_glt = getmnum(dom["dom"])[0:2]
            case "dipl_CMN":
                dipl_CMN = getmnum(dom["dom"])[0:2]
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
    write_to_github_step()
    write_to_github_step(
        "File\tdi_GLT\tdi_CMN\ted_GLT\t\ted_CMN\n"
        "\t".join(["|".join(f) if isinstance(f, tuple) else f for f in output_list])
    )

    return active_dom


def getmnum(root: etree.Element):
    """returns @n of last measure"""

    measures = root.xpath("//mei:measure", namespaces=ns)
    measures_invis = root.xpath("//mei:measure[@right='invis']", namespaces=ns)
    endings = root.xpath("//mei:ending", namespaces=ns)
    has_pickup = measures[0].get("type", "no") == "pickup"

    return (
        measures[-1].get("n", "no_n"),
        str(len(measures) - int(len(endings) / 2) - has_pickup),
        str(len(measures)),
        str(len(measures_invis)),
    )


def create_list(root: str, files: list):
    dipl_glt = ("fnf", 0)
    dipl_CMN = ("fnf", 0)
    ed_glt = ("fnf", 0)
    ed_CMN = ("fnf", 0)
    filename = ("error", 0)

    for file in files:
        filematch = re.match(
            r".+(n\d+)_([0-9rv-]+)_enc_((ed|dipl)_(CMN|GLT))\.mei", file
        )
        if filematch is not None:
            filename = filematch.group(1)
            match filematch.group(3):
                case "dipl_GLT":
                    dipl_glt = getmnum(os.path.join(root, file))[0:2]
                case "dipl_CMN":
                    dipl_CMN = getmnum(os.path.join(root, file))[0:2]
                case "ed_GLT":
                    ed_glt = getmnum(os.path.join(root, file))
                case "ed_CMN":
                    ed_CMN = getmnum(os.path.join(root, file))

    # return [filename,dipl_glt,dipl_CMN,ed_glt,ed_CMN] if not (dipl_glt[0]==dipl_CMN[0]==ed_glt[0]==ed_CMN[0] and dipl_glt[1]==dipl_CMN[1] and ed_glt[1]==ed_CMN[1] and ed_glt[2]==ed_CMN[2]=="0") else []
    return (
        [filename, ed_glt] if not (ed_glt[0] == ed_glt[1] and ed_glt[3] == "0") else []
    )


def add_header_from_GLT(
    active_dom: dict, context_doms: list, projectstaff: str, role: str, **addargs
):
    """
    Adds header from GLT

    :param active_dom: dict containing {filename:str, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """
    # TODO needs to be adjusted

    root = active_dom["dom"]
    for context_dom in context_doms:
        if context_dom["notationtype"] == "ed_GLT":
            helproot = context_dom["dom"]
            break
    else:
        raise ValueError("add_header_from_GLT needs context_dom ed_GLT, not given")

    if root.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        raise Exception(f"{active_dom["filename"]} already has header")

    if not helproot.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        raise Exception(f"{active_dom["filename"]} has no header")

    appInfo = root.find(".//mei:appInfo", namespaces=ns)

    header = copy.deepcopy(helproot.find(".//mei:meiHead", namespaces=ns))

    titlePart = header.find(".//mei:titlePart/mei:abbr", namespaces=ns)
    titlePart.clear()
    titlePart.set("expan", "Common Music Notation")
    titlePart.text = "CMN"

    edition = header.find(".//mei:edition", namespaces=ns)
    edition.set("resp", f"#{projectstaff}")
    edition.text = f"First {'diplomatic transcription' if 'dipl' in active_dom["notationtype"] else 'edition'} in CMN. Lute tuned in A."

    judenkunig = header.xpath(".//mei:persName[@xml:id='persons-78']", namespaces=ns)
    if judenkunig:
        judenkunig[0].set("role", role)

    appinfoold = header.find(".//mei:appInfo", namespaces=ns)
    encodingDesc = appinfoold.getparent()
    encodingDesc.remove(appinfoold)
    encodingDesc.insert(0, appInfo)

    edps = header.xpath(".//mei:editorialDecl//p", namespaces=ns)

    for edp in edps:
        edp.text = ""

    revisionDesc = header.find("./mei:revisionDesc", namespaces=ns)
    del revisionDesc[1:]
    revisionDesc[0].attrib.update({"isodate": "YYYY-MM-DD", "n": "1", "resp": "#"})
    revps = revisionDesc.xpath(".//mei:p", namespaces=ns)

    for revp in revps:
        revp.text = ""

    root.remove(root.find("./mei:meiHead", namespaces=ns))
    root.insert(0, header)

    active_dom["dom"] = root
    return active_dom
