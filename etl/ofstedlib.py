import dateparser

header = {
    "name": ["School name"],
    "postcode": ["Postcode"],
    "phase_of_education": ["Phase of education", "Ofsted Phase"],
    "overall_effectiveness": ["Overall effectiveness"],
    "ts": ["Publication date"],
}


def transform(csvheader, csvrow):
    if csvrow and csvrow[0]:
        try:
            header_indices = {}
            for k, v in header.items():
                for vi in v:
                    if vi in csvheader:
                        header_indices[k] = csvheader.index(vi)
                        break
            doc = {k: csvrow[i].strip() for (k, i) in header_indices.items()}
            doc["overall_effectiveness"] = float(doc["overall_effectiveness"])
            doc["ts"] = dateparser.parse(doc["ts"])
            return doc
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
