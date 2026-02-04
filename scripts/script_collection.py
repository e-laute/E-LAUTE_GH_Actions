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


def write_output(
    active_dom: dict, context_doms: list, output_path: Path = None, **addargs
):
    # TODO tree.write needs tree not root. Either all scripts do tree.getroot or
    # write output gets treated differenly
    """
    writes active_dom to output_path

    :param active_dom: dict containing {filename:Path/str?, notationtype:str, dom:etree.Element}
    :type active_dom: dict
    :param context_doms: list containing dom dicts
    :type context_doms: list
    :param output_path: path of file to write DOM
    :type output_path: Path
    :param addargs: Addional arguments that are unused
    """

    root = active_dom["dom"]

    # etree.indent(tree, "   ")

    # Write back, preserving XML declaration and processing instructions
    # with open(file, "wb") as f:
    # tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)

    active_dom["dom"] = root
    return active_dom


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


def function(active_dom: dict, context_doms: list, **addargs):
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
