from typing import Set

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


def get_places_from(csvrow) -> Set[str]:
    return set(
        [
            csvrow[header["locality"]],
            csvrow[header["town_or_city"]],
            csvrow[header["district"]],
            csvrow[header["county"]],
        ]
    )
