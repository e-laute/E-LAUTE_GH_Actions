import os
import re
from lxml import etree
import copy
import sys
import math
from utils import *


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}

def add_sbs_every_n(root:etree.Element,n:int=5):
    """Function description.

    Args:
      root: The root of the parsed tree of the MEI-file.
      n (optional): Int describing the number of measures between sb.

    Returns:
      The changed root.

    Raises:
      Error-type: Any potential Errors.
    """

    measures = root.xpath(".//mei:measure", namespaces=ns)

    count = n
    for measure in measures:
        if count == 5:
            sb = etree.Element("sb")
            parent = measure.getparent()
            parent.insert(parent.index(measure)+1,sb)
        else:
            count += 1

    
    return root


def remove_sbs(root:etree.Element):
    """Remove all sbs.

    Args:
      root: The root of the parsed tree of the MEI-file.

    Returns:
      The changed root.

    Raises:
      Error-type: Any potential Errors.
    """

    sbs = root.xpath(".//mei:sb", namespaces=ns)
    
    for sb in sbs:
        parent = sb.getparent()
        parent.remove(sb)

    return root    

def function(root:etree.Element):
    """Function description.

    Args:
      root: The root of the parsed tree of the MEI-file.

    Returns:
      The changed root.
      Optional: The output string containing the formatted information.

    Raises:
      Error-type: Any potential Errors.
    """

    output_str = ""

    xpath_result = root.xpath(".//mei:elem[@attrib='value']", namespaces=ns)
    
    return root,output_str