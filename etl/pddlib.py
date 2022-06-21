# https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
from typing import Tuple
import dateparser


header = {
    "price": 1,
    "ts": 2,
    "postcode": 3,
    "property_type": 4,
    "new_build": 5,
    "tenure_type": 6,
    "property_number_or_name": 7,
    "building_or_block": 8,
    "street_name": 9,
    "locality": 10,
    "town_or_city": 11,
    "district": 12,
    "county": 13,
    "ppd_category_type": 14,
}

property_type_names = {
    "D": "Detached",
    "S": "Semi-detached",
    "T": "Terraced",
    "F": "Flats/Maisonettes",
    "O": "Other",
}

tenure_names = {
    "F": "Freehold",
    "L": "Leasehold",
    "U": "Unspecified",
}


def fix_critical_positions(csvrow):
    postcode = csvrow[header["postcode"]]
    csvrow[header["postcode"]] = postcode if postcode else ""

    locality = csvrow[header["locality"]]
    csvrow[header["locality"]] = locality if locality else csvrow[header["district"]]

    property_type = csvrow[header["property_type"]]
    csvrow[header["property_type"]] = property_type_names[property_type if property_type else "O"]

    tenure_type = csvrow[header["tenure_type"]]
    csvrow[header["tenure_type"]] = tenure_names[tenure_type if tenure_type else "U"]

    csvrow[header["new_build"]] = csvrow[header["new_build"]] == "Y"

    try:
        csvrow[header["price"]] = float(csvrow[header["price"]])
    except:
        csvrow[header["price"]] = None

    try:
        csvrow[header["ts"]] = dateparser.parse(csvrow[header["ts"]])
    except:
        csvrow[header["ts"]] = None

    return csvrow


def get_localities_from(csvrow):
    return csvrow[header["locality"]]


def get_postcode_from(csvrow) -> str:
    return csvrow[header["postcode"]]

def get_locality_postcodes_from(csvrow) -> Tuple[str, str]:
    return (csvrow[header["locality"]], csvrow[header["postcode"]])

def get_property_type_from(csvrow):
    return csvrow[header["property_type"]]


def get_tenure_from(csvrow):
    return csvrow[header["tenure_type"]]


def get_property_from(csvrow):
    return (
        csvrow[header["postcode"]],
        csvrow[header["property_type"]],
        csvrow[header["property_number_or_name"]],
        csvrow[header["building_or_block"]],
        csvrow[header["street_name"]],
    )


def get_transaction_from(csvrow):
    return (
        (
            csvrow[header["postcode"]],
            csvrow[header["property_type"]],
            csvrow[header["property_number_or_name"]],
            csvrow[header["building_or_block"]],
            csvrow[header["street_name"]],
        ),
        csvrow[header["tenure_type"]],
        csvrow[header["new_build"]],
        csvrow[header["price"]],
        csvrow[header["ts"]],
    )
