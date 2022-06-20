# https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
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

property_type = {
    "D": "Detached",
    "S": "Semi-detached",
    "T": "Terraced",
    "F": "Flats/Maisonettes",
    "O": "Other",
}

tenure_type = {
    "F": "Freehold",
    "L": "Leasehold",
    "U": "Unspecified",
}


def fix_critical_positions(csvrow):
    postcode = csvrow[header["postcode"]]
    csvrow[header["postcode"]] = postcode if postcode else ""

    property_type = csvrow[header["property_type"]]
    csvrow[header["property_type"]] = property_type if property_type else "O"

    tenure_type = csvrow[header["tenure_type"]]
    csvrow[header["tenure_type"]] = tenure_type if tenure_type else "U"

    return csvrow


def get_places_from(csvrow):
    return set(
        [
            csvrow[header["locality"]],
            csvrow[header["town_or_city"]],
            csvrow[header["district"]],
            csvrow[header["county"]],
        ]
    )


def get_postcode_from(csvrow) -> str:
    return csvrow[header["postcode"]]


def update_map_with_postgroups_and_place_links(append_to, csvrow):
    postcode = csvrow[header["postcode"]]
    postcode = postcode if postcode else ""
    if postcode:
        postgroup = postcode.split(" ")[0]
        append_to.setdefault(postgroup, set())
        place_set = append_to[postgroup]
        place_set.add(csvrow[header["locality"]])
        place_set.add(csvrow[header["town_or_city"]])
        place_set.add(csvrow[header["district"]])
        place_set.add(csvrow[header["county"]])


def get_property_type_from(csvrow):
    return csvrow[header["property_type"]]


def get_property_from(csvrow):
    return (
        csvrow[header["postcode"]],
        property_type[csvrow[header["property_type"]]],
        csvrow[header["property_number_or_name"]],
        csvrow[header["building_or_block"]],
        csvrow[header["street_name"]],
    )
