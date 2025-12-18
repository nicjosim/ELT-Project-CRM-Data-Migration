# rules.py
import re

# Date parsing
DATE = {
    "dayfirst": True,          # NZ/AU style
}

# Phone formatting
PHONE = {
    "digits_only": True,
    "min_len": 8,             
}

# Email formatting
EMAIL = {
    "lower": True,
    "strip": True,
}

# Percent formatting
PERCENT = {
    "fraction_to_percent": True,   
    "decimals": 2,
}

CITY_TO_COUNTRY = {
    # Australia (major)
    "sydney": "Australia",
    "melbourne": "Australia",
    "brisbane": "Australia",
    "perth": "Australia",
    "adelaide": "Australia",
    "canberra": "Australia",
    "gold coast": "Australia",
    "newcastle": "Australia",
    "hobart": "Australia",
    "darwin": "Australia",

    # New Zealand (major)
    "auckland": "New Zealand",
    "wellington": "New Zealand",
    "christchurch": "New Zealand",
    "hamilton": "New Zealand",
    "tauranga": "New Zealand",
    "dunedin": "New Zealand",
}

# Country normalization rules
COUNTRY = {
    "map": {
        "AU": "Australia",
        "AUS": "Australia",
        "AUSTRALIA": "Australia",
        "TASMANIA": "Australia",
        "NSW": "Australia",
        "VIC": "Australia",
        "QLD": "Australia",
        "SA": "Australia",
        "WA": "Australia",
        "TAS": "Australia",
        "ACT": "Australia",
        "NT": "Australia",
        "NZ": "New Zealand",
        "NZL": "New Zealand",
        "NEW ZEALAND": "New Zealand",
    }
}

# Address normalization rules
ADDRESS = {
    "po_box_patterns": [
        r"\bP\.?\s*O\.?\s*BOX\b",
    ],
    "po_box_replacement": "PO BOX",
    "components": {
        "unit": r"\bUnit\s+([A-Za-z0-9]+)\b",
        "suite": r"\b(Suite|Ste)\s+([A-Za-z0-9]+)\b",
        "level": r"\b(Level|Lvl)\s+([A-Za-z0-9]+)\b",
        "unit_slash": r"^\s*([A-Za-z0-9]+)\s*/\s*(\d+)\s+(.*)$",  # 4/12 Park Ave
    },
    "street_abbrev": {
        "st": "Street",
        "rd": "Road",
        "ave": "Avenue",
        "dr": "Drive",
        "ln": "Lane",
        "ct": "Court",
    },
    # Output order for extracted components
    "order": ["unit", "base", "level", "suite"],
}

OUT_COLS = [
    "Account ID", "First Name", "Last Name", "Email", "Country Code", "Phone Number",
    "Date of Birth", "Address Line", "Suburb", "Postcode", "City", "Country",
    "PIR %", "WHT %", "Tax Identification Number"
]

MERGE_COLS = [
    "First Name", "Last Name", "Email", "Country Code", "Phone Number", "Date of Birth",
    "Address Line", "Suburb", "Postcode", "City", "Country",
    "PIR %", "WHT %", "Tax Identification Number"
]

PHONE_PREFIX = ("61", "64")  # AU, NZ

COUNTRY_TO_CC = {
    "Australia": "61",
    "New Zealand": "64",
}