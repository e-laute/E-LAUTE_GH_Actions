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

    app_Info = root.find(".//mei:appInfo", namespaces=ns)

    application = etree.SubElement(app_Info, "application", {"isodate":date.today().isoformat()})
    name = etree.SubElement(application,"name")
    name.text = "Github-Action Script"
    p = etree.SubElement(application,"p")
    name.text = p_description
    
    return root