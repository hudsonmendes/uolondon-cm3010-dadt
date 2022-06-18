# https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
header = {
    "price": 1,
    "date_of_transfer": 2,
    "postcode": 3,
    "property_type": 4,
    "old_or_new": 5,
    "tenure_type": 6,
    "property_number_nr_name": 7,
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


def transform(csvrow):
    doc = {k: csvrow[i] for (k, i) in header.items()}
    if doc["ppd_category_type"] == "A":
        doc["property_type"] = property_type.get(doc["property_type"], property_type.get("O"))
        doc["tenure_type"] = property_type.get(doc["tenure_type"], property_type.get("U"))
        return doc
