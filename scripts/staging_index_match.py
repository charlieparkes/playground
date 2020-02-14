import re


def get_staging_index_from_og_id(id):
    if id[:3] == "PAT":
        if id[4:7] == "PFB":
            index = "pfb"
        else:
            index = "pat"
    else:
        index = id[:3].lower()
    return f"{index}_og"


def get_staging_index_from_og_id_regex(id):
    def _index(prefix):
        return f"{prefix}_og"
    groups = [g.lower() for g in re.findall('([A-Z]+)', id)]
    assert len(groups) >= 1
    if set(["pat", "pfb"]).issubset(groups):
        return _index("pfb")
    return _index(groups[0])

ids = [
    ("PAT:MUL/35678913418", "pat_og"),
    ("PAT:PFB/356789134asdfasdf18", "pfb_og"),
    ("MIN:DRM/20200131-011870", "min_og"),
    ("EDS:ab15cfd896d7d9b1e8d286c58950d7af", "eds_og")
]

print("slicing")
for id, index in ids:
    result = get_staging_index_from_og_id(id)
    print(f"id: {id}, index: {index}, result: {result}")
    assert result == index

print("regex")
for id, index in ids:
    result = get_staging_index_from_og_id_regex(id)
    print(f"id: {id}, index: {index}, result: {result}")
    assert result == index
