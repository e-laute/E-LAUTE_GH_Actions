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