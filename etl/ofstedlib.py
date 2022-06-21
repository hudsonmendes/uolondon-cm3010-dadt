import dateparser

header = {
    "name": ["SCHOOL NAME"],
    "postcode": ["POSTCODE"],
    "phase_of_education": ["PHASE OF EDUCATION", "OFSTED PHASE"],
    "overall_effectiveness": ["OVERALL EFFECTIVENESS"],
    "ts": ["PUBLICATION DATE"],
}


def transform(csvheader, csvrow):
    if csvrow and csvrow[0]:
        try:
            header_indices = {}
            for k, v in header.items():
                for vi in v:
                    if vi in csvheader:
                        header_indices[k] = csvheader.index(vi.upper())
                        break
                if not k in header_indices:
                    raise KeyError(f"None of the keys for '{k}' found")
            doc = {k: csvrow[i].strip() for (k, i) in header_indices.items()}
            if doc["overall_effectiveness"] != "NULL":
                doc["overall_effectiveness"] = float(doc["overall_effectiveness"])
                doc["ts"] = dateparser.parse(doc["ts"])
                return doc
            else:
                return None
        except Exception as e:
            print((e, csvheader))


def get_education_phases_from(csvrecords):
    return set(r["phase_of_education"] for r in csvrecords)


def get_schools_from(csvrecords):
    return set((r["postcode"], r["name"]) for r in csvrecords)


def get_school_ratings_from(csvrecords):
    return set(
        (r["postcode"], r["name"], r["phase_of_education"], r["overall_effectiveness"], r["ts"]) for r in csvrecords
    )
