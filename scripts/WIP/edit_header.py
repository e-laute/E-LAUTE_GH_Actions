import os
import re
from lxml import etree as ET
import copy
import sys


ns = {"mei":"http://www.music-encoding.org/ns/mei",
        "xml":"http://www.w3.org/XML/1998/namespace"}



def edit_header(file:str):
    """edits header for multiple one time changes"""
    
    with open(file, "rb") as f:
        tree = ET.parse(f,ET.XMLParser(recover=True))
    root = tree.getroot()

    title_data=re.search(r"(Jud[^\\]+_n\d+)_([^\\_]+)_enc_(ed|dipl)_(GLT|CMN)",file).groups()

    print(title_data[0],title_data[1])
    """
    analytic = root.find(".//mei:analytic", namespaces=ns)
    ana_id = analytic.find("./mei:identifier",namespaces=ns)
    if ana_id.text == "" or ana_id.text.isspace():
        ana_id.text = title_data[0]
    ana_id_comms = ana_id.xpath("./comment()",namespaces=ns)
    for ana_id_comm in ana_id_comms:
        if ana_id_comm.text == " E-LAUTE ID ":
            ana_id.remove(ana_id_comm)

    ana_bib = analytic.find("./mei:biblScope",namespaces=ns)
    if ana_bib.text == "" or ana_bib.text.isspace():
        ana_bib.text = title_data[1]
    ana_bib_comms = ana_bib.xpath("./comment()",namespaces=ns)
    for ana_bib_comm in ana_bib_comms:
        if ana_bib_comm.text == ' here comes the db field "Fols. / p. new" ':
            ana_bib.remove(ana_bib_comm)

    pup_id = root.xpath(".//mei:pubStmt/mei:identifier",namespaces=ns)[0]
    if pup_id.text.startswith("o:lau.") or pup_id.text == "" or pup_id.text.isspace():
        pup_id.text = f"o:lau.{title_data[0]}"
    pup_id_comms = pup_id.xpath("./comment()",namespaces=ns)
    for pup_id_comm in pup_id_comms:
        if pup_id_comm.text == " E-LAUTE ID ":
            pup_id.remove(pup_id_comm)
    """

    # add link to conventions to edDcl
    edDcl = root.xpath(".//mei:editorialDecl",namespaces=ns)
    if not edDcl:
        print("".join(title_data)," has no editorialDecl")
    else:
        edDcl = edDcl[0]
        """
        p_comms = edDcl[0].xpath("./comment()",namespaces=ns)
        for p_comm in p_comms:
            if p_comm.text == " add a declaration that links to the editorial guidelines ":
                edDcl[0].remove(p_comm)
        if len(edDcl) == 0 or (
            edDcl[0].text is not None
            and not (edDcl[0].text.isspace() or edDcl[0].text == "")
            ):
            edDcl.insert(0,ET.Element("p"))
        edDcl[0].text = "E-LAUTE Edition Guidelines: https://edition.onb.ac.at/fedora/objects/o:lau.red-editionguidelines/datastreams/MEI_CONVENTIONS/content" 
        edDcl.append(ET.Element("p"))
        edDcl[-1].text="Every rest is encoded as <space/>"
        """

    author = root.xpath(".//mei:analytic//mei:persName[@role='scribe']",namespaces=ns)
    if author:
        author[0].set("role","intabulator")

    if not root.xpath(".//mei:persName[@auth.uri='https://e-laute.info/data/projectstaff/1']",namespaces=ns):
        resp_comment = root.xpath(".//mei:titleStmt//mei:respStmt/comment()[.=' meiHead modelers ']",namespaces=ns)
        print(resp_comment)
        index = resp_comment[0].getparent().index(resp_comment[0]) if resp_comment else 1

        person = ET.Element("persName",{"role":"meiEditor", "auth.uri":"https://e-laute.info/data/projectstaff/1", f"{{{ns['xml']}}}id":"projectstaff-1"})
        fore = ET.SubElement(person,"foreName")
        fam = ET.SubElement(person,"famName")
        fore.text="Kateryna"
        fam.text="Schöning"
        respstmt=root.find(".//mei:titleStmt/mei:respStmt",namespaces=ns)
        respstmt.insert(index,person)
    
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
            if re.fullmatch(r"Jud.*_enc_(ed|dipl)_(GLT|CMN)\.mei",file)!=None:
                edit_header(os.path.join(root,file))

choosefile()