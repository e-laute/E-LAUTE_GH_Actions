import os
import re
from lxml import etree
import copy
import sys
import math
from datetime import date


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

    

def edit_appInfo(root:etree.Element,p_description:str):
    """Adds <application> to <appInfo> with <p> containing p_description.

    Args:
      root: The root of the parsed tree of the MEI-file.
      p_description: String to be added to a <p>-elem under <application>.

    Returns:
      The changed root.

    Raises:
      Error-type: Any potential Errors.
    """

    applications = root.xpath(".//mei:application/name[text()='Github Action Scripts']", namespaces=ns)

    if not applications:
        app_Info = root.find(".//mei:appInfo", namespaces=ns)
        application = etree.SubElement(app_Info, "application", {"isodate":date.today().isoformat()})
        name = etree.SubElement(application,"name")
        name.text = "Github Action Scripts"
    else:
        application = applications[0]
        if application.get("isodate") is not None:
            application.set("startdate",application.get("isodate"))
            application.attrib.pop("isodate")
        application.set("enddate",date.today().isoformat())

    p = etree.SubElement(application,"p")
    p.text = p_description
    
    return root

def dur_length(elem:etree.Element,ignore=["sic","orig"]):
    """Recursively adds up all @dur in subtree while accounting for @dots.

    Args:
      elem: Root of a MEI-Subtree.
      ignore (optional): List of elements to not count; defaults to orig and sic to avoid choice duplication

    Returns:
      Float which represents the combined dur in Quavers.

    Raises:
      Error-type: Any potential Errors.
    """

    totaldur = 0.
    for child in elem:
        if etree.QName(child).localname in ignore:
            continue
        if "dur" in child.attrib:
            dur = float(child.attrib.get("dur"))
            totaldur += 2/dur - 1/(dur*2**int(child.attrib.get("dots","0")))
        else:
            totaldur+=dur_length(child)
    return totaldur
