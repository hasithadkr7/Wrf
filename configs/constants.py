METRO_COLOMBO_EXTENT = [79.8561, 6.84214, 79.9746, 6.96515]
COLOMBO_EXTENT = [79.820, 6.83265, 79.9845, 6.99485]
KELANI_UPPER_BASIN_EXTENT = [79.994117, 6.754167, 80.773182, 7.229167]
KELANI_LOWER_BASIN_EXTENT = [79.8389, 6.77083, 80.1584, 7.04713]
SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]
KELANI_KALU_BASIN_EXTENT = [79.8289, 6.53, 80.7832, 7.23917]
SRI_LANKA_D01_EXTENT = [71.2166, -3.06107, 90.3315, 18.1895]

WT_NAMELISTS = {
    "H": [
        "namelist.input_H",
        "namelist.input_SIDAT",
        "namelist.input_C",
        "namelist.input_W"
    ],
    "A": [
        "namelist.input_SIDAT",
        "namelist.input_H",
        "namelist.input_C",
        "namelist.input_W"
    ],
    "C": [
        "namelist.input_C",
        "namelist.input_H",
        "namelist.input_SIDAT",
        "namelist.input_W"
    ],
    "E": [
        "namelist.input_SIDAT",
        "namelist.input_H",
        "namelist.input_W",
        "namelist.input_NW"
    ],
    "SE": [
        "namelist.input_SIDAT",
        "namelist.input_H",
        "namelist.input_C",
        "namelist.input_W"
    ],
    "W": [
        "namelist.input_W",
        "namelist.input_H",
        "namelist.input_SIDAT",
        "namelist.input_SW"
    ],
    "N": [
        "namelist.input_SIDAT",
        "namelist.input_H",
        "namelist.input_NW",
        "namelist.input_W"
    ],
    "NE": [
        "namelist.input_SIDAT",
        "namelist.input_H",
        "namelist.input_NW",
        "namelist.input_W"
    ],
    "NW": [
        "namelist.input_NW",
        "namelist.input_H",
        "namelist.input_W",
        "namelist.input_SIDAT"
    ],
    "S": [
        "namelist.input_SIDAT",
        "namelist.input_C",
        "namelist.input_H",
        "namelist.input_SW"
    ],
    "SW": [
        "namelist.input_SW",
        "namelist.input_H",
        "namelist.input_SIDAT",
        "namelist.input_W"
    ],
    "default": [
        "namelist.input_C",
        "namelist.input_H",
        "namelist.input_NW",
        "namelist.input_SW",
        "namelist.input_W"
    ]
}

# constants for Zip file compression methods
ZIP_STORED = 0
ZIP_DEFLATED = 8
ZIP_BZIP2 = 12
ZIP_LZMA = 14
# Other ZIP compression methods not supported

DEFAULT_VERSION = 20
ZIP64_VERSION = 45
BZIP2_VERSION = 46
LZMA_VERSION = 63
# we recognize (but not necessarily support) all features up to that version
MAX_EXTRACT_VERSION = 63

