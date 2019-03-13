from enum import Enum


class Station(Enum):
    """
    Station ids ranged as below;
    - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
    - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
    - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
    - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
    - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)
    Simulation models station ids ranged over 1’000’000 as below;
    - 1 1xx xxx - WRF (stationId: wrf_<SOMETHING>)
    - 1 2xx xxx - FLO2D (stationId: flo2d_<SOMETHING>)
    Other;
    - 2 xxx xxx - Other (stationId: other_<SOMETHING>)
    """
    CUrW = 100000
    Megapolis = 200000
    Government = 300000
    Gov = 300000
    Public = 400000
    Satellite = 800000
    Sat = 800000

    WRF = 1100000
    FLO2D = 1200000
    MIKE11 = 1300000

    Other = 2000000

    _nameToRange = {
        CUrW: 100000,
        Megapolis: 100000,
        Government: 100000,
        Gov: 100000,
        Public: 400000,
        Satellite: 200000,
        Sat: 200000,

        WRF: 100000,
        FLO2D: 100000,
        MIKE11: 100000,

        Other: 1000000
    }

    @staticmethod
    def getRange(name):
        _nameToRange = {
            Station.CUrW: 100000,
            Station.Megapolis: 100000,
            Station.Government: 100000,
            Station.Gov: 100000,
            Station.Public: 400000,
            Station.Satellite: 200000,
            Station.Sat: 200000,

            Station.WRF: 100000,
            Station.FLO2D: 100000,
            Station.MIKE11: 100000,

            Station.Other: 1000000
        }
        return _nameToRange.get(name, 1000000)

    @staticmethod
    def getType(name):
        _nameToType = {
            'CUrW': Station.CUrW,
            'Megapolis': Station.Megapolis,
            'Government': Station.Government,
            'Gov': Station.Gov,
            'Public': Station.Public,
            'Satellite': Station.Satellite,
            'Sat': Station.Sat,

            'WRF': Station.WRF,
            'FLO2D': Station.FLO2D,
            'MIKE11': Station.MIKE11,

            'Other': Station.Other
        }
        return _nameToType.get(name, Station.Other)

