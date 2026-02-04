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

    count = sbInterval
    for measure in measures:
        if count == 5:
            sb = etree.Element("sb")
            parent = measure.getparent()
            parent.insert(parent.index(measure) + 1, sb)
        else:
            count += 1

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


def add_header_from_GLT(
    active_dom: dict, context_doms: list, projectstaff: str, role: str, **addargs
):
    """
    template function

    :param active_dom: dict containing {filename:str, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param addargs: Addional arguments that are unused
    """

    root = active_dom["dom"]
    for context_dom in context_doms:
        if context_dom["notationtype"] == "ed_GLT":
            helproot = context_dom["dom"]
            break
    else:
        raise ValueError("add_header_from_GLT needs context_dom ed_GLT, not given")

    """    
    if root.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        print(f"{active_dom["filename"]} already has header")
        return active_dom
    """

    if not helproot.xpath(
        ".//mei:corpName//mei:expan[text()='Electronic Linked Annotated Unified Tablature Edition']",
        namespaces=ns,
    ):
        print(f"{active_dom["filename"]} has no header")
        return active_dom

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
