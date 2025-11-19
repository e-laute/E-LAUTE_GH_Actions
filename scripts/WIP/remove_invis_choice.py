import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace",
        "re":"http://exslt.org/regular-expressions"}

def subtrees_equal(e1, e2, ignore_attrs=('{http://www.w3.org/XML/1998/namespace}id','right','startid','endid'), _is_root=True):
    """
    Compare two lxml elements for subtree equality, ignoring @xml:id,
    while allowing the *root* elements to differ entirely.

    Parameters
    ----------
    e1, e2 : lxml.etree._Element
        Elements whose subtrees will be compared.
    ignore_attrs : tuple of str
        Attribute names to ignore during comparison.
    _is_root : bool (internal)
        Indicates whether this is the initial call.

    Returns
    -------
    bool
        True if subtrees are identical except for ignored attributes,
        allowing the roots themselves to be completely different.
    """

    # If we are *not* at the root, tags must match
    if not _is_root and e1.tag != e2.tag:
        return False

    # If we are not at root, compare attributes (excluding ignored ones)
    if not _is_root:
        attrs1 = {k: v for k, v in e1.attrib.items() if k not in ignore_attrs}
        attrs2 = {k: v for k, v in e2.attrib.items() if k not in ignore_attrs}
        if attrs1 != attrs2:
            return False

        # Compare text and tail (normalized)
        if (e1.text or '').strip() != (e2.text or '').strip():
            return False
        if (e1.tail or '').strip() != (e2.tail or '').strip():
            return False

    # Compare children count
    c1 = list(e1)
    c2 = list(e2)
    if len(c1) != len(c2):
        return False

    # Recursively compare children, but no longer root
    return all(
        subtrees_equal(a, b, ignore_attrs, _is_root=False)
        for a, b in zip(c1, c2)
    )


def fol_dir_fontsize(file:str):
    """remove choice containing whole measure"""

    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    candidates = root.xpath(".//mei:choice[descendant::mei:measure]", namespaces=ns)

    for candidate in candidates:
        sic=candidate.find("./mei:sic", namespaces=ns)
        corr=candidate.find("./mei:corr", namespaces=ns)
        if sic is not None and corr is not None and subtrees_equal(sic,corr):
            measure = copy.deepcopy(candidate.find("./mei:corr//mei:measure", namespaces=ns))
            parent = candidate.getparent()
            index = parent.index(candidate)
            parent.remove(candidate)
            parent.insert(index,measure)
        
    ET.register_namespace("mei", ns["mei"])
    ET.register_namespace("xml", ns["xml"])   

    ET.indent(tree,"   ")     

    # Write back, preserving XML declaration and processing instructions
    with open(file, "wb") as f:
        tree.write(f, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def choosefile():
    dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    for root, dirs, files in os.walk(dir_path):
        if "converted" in root:
            continue
        for file in files:
            if re.fullmatch(r"Jud.+n\d+.+_(ed)_CMN\.mei",file)!=None:
                fol_dir_fontsize(os.path.join(root,file))


choosefile()