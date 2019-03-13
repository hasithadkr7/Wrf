import csv
import logging
from random import random
from tempfile import TemporaryDirectory
from netCDF4 import Dataset
import numpy as np
import os
import datetime as dt
from configs import constants
from utils import utils as ext_utils
from configs import manager as res_mgr
from configs import station as Station


# extract_data_wrf INFO Running arguments:'
# '{'
# '"data_type": "data",'
# '"db_config": "eyAiaG9zdCI6ICIxOTIuMTY4LjEuNDIiLCAidXNlciI6ICJjdXJ3IiwgInBhc3N3b3JkIjogImN1cnciLCAiZGIiOiAiY3VydyIgfQ==",'
# '"overwrite": "True",'
# '"procedures": "9223372036854775807",'
# '"run_id": "wrf0_2019-03-09_18:00_0000",'
# '"wrf_config": "eyJhcmNoaXZlX2RpciI6ICIvd3JmL2FyY2hpdmUiLCAiZ2VvZ19kaXIiOiAiL3dyZi9nZW9nIiwgImdmc19kaXIiOiAiL3dyZi9nZnMiLCAibmZzX2RpciI6ICIvd3JmL291dHB1dCIsICJwZXJpb2QiOiAzLCAicHJvY3MiOiA0LCAicnVuX2lkIjogIndyZjBfMjAxOS0wMy0wOV8xODowMF8wMDAwIiwgInN0YXJ0X2RhdGUiOiAiMjAxOS0wMy0wOV8xODowMCIsICJ3cHNfcnVuX2lkIjogIndyZjBfMjAxOS0wMy0wOV8xODowMF8wMDAwIiwgIndyZl9ob21lIjogIi93cmYifQ=="'
# '}'
def push_wrf_rainfall_to_db(nc_f, lon_min=None, lat_min=None, lon_max=None, lat_max=None, station_prefix='wrf'):
    """

    :param run_name:
    :param nc_f:
    :param curw_db_adapter: If not none, data will be pushed to the db
    :param run_prefix:
    :param lon_min:
    :param lat_min:
    :param lon_max:
    :param lat_max:
    :param upsert:
    :return:
    """

    if not all([lon_min, lat_min, lon_max, lat_max]):
        lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT

    nc_vars = ext_utils.extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']
    print('lats : ', lats)
    print('lons : ', lons)
    # print('prcp : ', prcp)
    print('times : ', times)

    diff = ext_utils.get_two_element_average(prcp)
    # print('diff : ', diff)

    width = len(lons)
    height = len(lats)
    print('width : ', width)
    print('height : ', height)
    rf_ts = {}
    # for y in range(height):
    #     for x in range(width):
    #         lat = lats[y]
    #         lon = lons[x]
    #
    #         station_id = '%s_%.6f_%.6f' % (station_prefix, lon, lat)
    #         name = station_id
    #
    #         print('Creating station %s ...' % name)
    #         station = [Station.WRF, station_id, name, str(lon), str(lat), str(0), "WRF point"]
    #         print('station : ', station)
    #
    #         # add rf series to the dict
    #         ts = []
    #         for i in range(len(diff)):
    #             t = ext_utils.datetime_utc_to_lk(dt.datetime.strptime(times[i], '%Y-%m-%d_%H:%M:%S'), shift_mins=30)
    #             ts.append([t.strftime('%Y-%m-%d %H:%M:%S'), diff[i, y, x]])
    #         rf_ts[name] = ts
    #         print('rf_ts : ', rf_ts)


if __name__ == "__main__":
    output_dir = '/home/hasitha/PycharmProjects/Wrf/wrf_output/muditha/NED'
    net_cdf_file = 'd03__A.nc'
    net_cdf_file_path = os.path.join(output_dir, net_cdf_file)
    wrf_output = output_dir
    push_wrf_rainfall_to_db(net_cdf_file_path)

