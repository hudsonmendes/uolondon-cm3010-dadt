import dateparser

header = {"name": 4, "postcode": 9, "phase_of_education": 11, "overall_effectiveness": 34, "ts": 24}


def transform(csvrow):
    doc = {k: csvrow[i].strip() for (k, i) in header.items()}
    doc["overall_effectiveness"] = float(doc["overall_effectiveness"])
    doc["ts"] = dateparser.parse(doc["ts"])
    return doc
