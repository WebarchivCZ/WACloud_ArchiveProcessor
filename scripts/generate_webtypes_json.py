#!/usr/bin/python
# coding: utf-8

"""Generate a JSON file with actual web page types.

The JSON can be used to update HBase row in config table.

"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from WebPageTypeIdentification import WebPageTypeIdentifier  # noqa: E402


wpti = WebPageTypeIdentifier()
output_path = os.path.join(
    os.path.dirname(__file__),
    "../config_table/WebPageTypeIdentification.json"
)
wpti.generate_webtypes_json(output_path)
print(f"JSON file with actual web page types saved to {output_path}")
