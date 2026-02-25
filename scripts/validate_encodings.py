# This python script recursively traverses a directory structure,
# finding all .mei files and validating them against MEI's RNG schema using the `lxml` library.
import os
import sys
import io
import argparse
from lxml import etree
import requests


def validate_mei_file(file_path, schema, errors):
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        doc = etree.fromstring(content)
        schema.assertValid(doc)
    except etree.DocumentInvalid as e:
        error_msg = f"Validation failed for {file_path}: {e}"
        errors.append(error_msg)
    except Exception as e:
        error_msg = f"Error processing {file_path}: {e}"
        errors.append(error_msg)


def find_mei_files(directory):
    mei_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".mei") or file.endswith(".tei"):
                mei_files.append(os.path.join(root, file))
    return mei_files


def main(directory, verbose=False):
    schemas = dict()
    errors = []
    mei_files = find_mei_files(directory)
    if not mei_files:
        if verbose:
            print(f"No .mei/.tei files found in: {directory}")
        return True

    for mei_file in mei_files:
        # read file into DOM and determine schema URL
        try:
            with open(mei_file, "rb") as f:
                content = f.read()
            doc = etree.fromstring(content)
            # determine schema URL from the document
            # checking in <?xml-model?> declaration
            schema_url = None
            # Get the root element and look for xml-model processing instructions
            root = doc.getroottree().getroot()
            for pi in root.xpath('//processing-instruction("xml-model")'):
                # Parse the pseudo-attributes from the processing instruction
                pi_text = pi.text
                if pi_text and "href=" in pi_text:
                    # Simple parsing of href attribute
                    import re

                    href_match = re.search(r'href=["\']([^"\']+)["\']', pi_text)
                    if href_match:
                        schema_url = href_match.group(1)
                        break
            if not schema_url:
                errors.append(f"No schema URL found in {mei_file}.")
                continue
            if schema_url not in schemas:
                # fetch the schema file using requests
                schema_response = requests.get(schema_url)
                if schema_response.status_code != 200:
                    errors.append(
                        f"Error fetching schema from {schema_url}: {schema_response.status_code}"
                    )
                    continue
                schema_file = io.BytesIO(schema_response.content)
                try:
                    schemas[schema_url] = etree.RelaxNG(
                        etree.parse(schema_file)
                    )
                except etree.XMLSyntaxError as e:
                    errors.append(
                        f"Error loading schema from {schema_url}: {e}"
                    )
                    continue
            validate_mei_file(mei_file, schemas[schema_url], errors)
        except etree.XMLSyntaxError as e:
            errors.append(f"Error parsing {mei_file}: {e}")
            continue

    if errors:
        if verbose:
            print(
                f"Validation failed in {directory} with {len(errors)} issue(s):",
                file=sys.stderr,
            )
            for error in errors:
                print(f"- {error}", file=sys.stderr)
        return False

    if verbose:
        print(f"Validation OK: {len(mei_files)} file(s) checked in {directory}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Validate all .mei/.tei files in a directory recursively against "
            "their declared RelaxNG schemas."
        )
    )
    parser.add_argument(
        "directory",
        nargs="?",
        help=(
            "Directory to validate. If omitted, uses the "
            "CALLER_REPO_PATH environment variable."
        ),
    )
    args = parser.parse_args()

    directory = args.directory or os.environ.get("CALLER_REPO_PATH")
    if not directory:
        print(
            "Error: provide a directory argument or set CALLER_REPO_PATH.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.isdir(directory):
        print(f"Error: not a valid directory: {directory}", file=sys.stderr)
        sys.exit(1)
    success = main(directory, verbose=True)
    if not success:
        sys.exit(1)
    sys.exit(0)
