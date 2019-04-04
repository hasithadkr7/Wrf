import shutil
import traceback
from random import random
import pandas as pd
from curwmysqladapter import Station
from netCDF4 import Dataset
import numpy as np
import os
import json
import gzip
from datetime import datetime, timedelta
import logging
from db_adapter.base import get_engine, get_sessionmaker
from db_adapter.timeseries import Timeseries
from db_adapter.station.station_utils import StationUtils
from db_adapter.station.station_enum import StationEnum
from db_adapter.source.source_utils import SourceUtils
SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]
# [lon_min, lat_min, lon_max, lat_max] : [79.5214614868164, 5.722969055175781, 82.1899185180664, 10.064254760742188]


# Can used to upload data to new database.


def push_rainfall_to_db(db_adapter, timeseries_dict, upsert=False, source='WRF',
                        source_params='{}', name='WRFv3_A'):
    for station, timeseries in timeseries_dict.items():
        print('Pushing data for station ' + station)
        meta_data = {
            'sim_tag': '',
            'scheduled_date': '',
            'latitude': station.split('_')[3],
            'longitude': station.split('_')[4],
            'model': station.split('_')[0],
            'version': station.split('_')[1],
            'variable': '',
            'unit': '',
            'unit_type': ''
        }
        print(meta_data)


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def random_check_stations_exist(station):
    query = {'name': station}
    if curw_db_adapter.get_station(query) is None:
        logging.debug('Random stations check fail')
        return False
    else:
        logging.debug('Random stations check success')
        return True


def read_netcdf_file(db_adapter, station_util, rainc_net_cdf_file_path,
                     rainnc_net_cdf_file_path, station_prefix,
                     run_name, upsert=False):
    print('rainc_net_cdf_file_path : ', rainc_net_cdf_file_path)
    print('rainnc_net_cdf_file_path : ', rainnc_net_cdf_file_path)
    print('station_prefix : ', station_prefix)
    if not os.path.exists(rainc_net_cdf_file_path):
        print('no rainc netcdf')
    elif not os.path.exists(rainnc_net_cdf_file_path):
        print('no rainnc netcdf')
    else:
        station_util = StationUtils(db_adapter.Session)
        nc_fid = Dataset(rainc_net_cdf_file_path, mode='r')
        rainc_unit_info = nc_fid.variables['RAINC'].units
        print('rainc_unit_info: ', rainc_unit_info)
        lat_unit_info = nc_fid.variables['XLAT'].units
        print('lat_unit_info: ', lat_unit_info)
        time_unit_info = nc_fid.variables['XTIME'].units
        print('time_unit_info: ', time_unit_info)
        time_unit_info_list = time_unit_info.split(' ')

        lats = nc_fid.variables['XLAT'][0, :, 0]
        lons = nc_fid.variables['XLONG'][0, 0, :]

        # lon_min, lat_min, lon_max, lat_max = SRI_LANKA_EXTENT
        lon_min = lons[0].item()
        lat_min = lats[0].item()
        lon_max = lons[-1].item()
        lat_max = lats[-1].item()
        print('[lon_min, lat_min, lon_max, lat_max] :', [lon_min, lat_min, lon_max, lat_max])
        lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
        lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

        rainc = nc_fid.variables['RAINC'][:, lat_inds[0], lon_inds[0]]

        nnc_fid = Dataset(rainnc_net_cdf_file_path, mode='r')
        rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

        times = nc_fid.variables['XTIME'][:]

        prcp = rainc + rainnc
        nc_fid.close()
        nnc_fid.close()
        diff = get_two_element_average(prcp)

        width = len(lons)
        height = len(lats)
        rf_ts = {}
        for y in range(height):
            for x in range(width):
                lat = lats[y]
                lon = lons[x]

                station_id = '%s_%.6f_%.6f' % (station_prefix, lon, lat)
                name = station_id
                stations_exists = random_check_stations_exist(name)
                if not stations_exists:
                    station_util.add_station(name, lat, lon, "WRF point", StationEnum.WRF)
                # add rf series to the dict
                ts = []
                for i in range(len(diff)):
                    ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                        minutes=times[i].item())
                    t = datetime_utc_to_lk(ts_time, shift_mins=30)
                    ts.append([t.strftime('%Y-%m-%d %H:%M:%S'), diff[i, y, x]])
                data_frame = pd.DataFrame(ts, columns=['time', 'value']).set_index(keys='time')
                rf_ts[name] = data_frame
        push_rainfall_to_db(db_adapter, rf_ts,
                            source=station_prefix,
                            upsert=upsert, name=run_name)


if __name__ == "__main__":
    current_time1 = datetime.datetime.now()
    try:
        config = json.loads(open('/home/uwcc-admin/netcdf_data_uploader/config.json').read())
        wrf_dir = '/mnt/disks/wrf-mod'
        wrf_version = '3'
        wrf_model_list = 'A,C,E,SE'
        start_date = ''
        if 'wrf_dir' in config:
            wrf_dir = config['wrf_dir']
        if 'start_date' in config:
            start_date = config['start_date']
        if 'wrf_version' in config:
            wrf_version = config['wrf_version']
        if 'wrf_model_list' in config:
            wrf_model_list = config['wrf_model_list']
        wrf_model_list = wrf_model_list.split(',')

        if start_date:
            run_date_str = (datetime.strptime(start_date,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            run_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        print('run_date_str : ', run_date_str)
        daily_dir = 'STATIONS_{}'.format(run_date_str)

        output_dir = os.path.join(wrf_dir, daily_dir)

        db_engine = get_engine(
            host=config['host'],
            port=3306,
            user=config['user'],
            password=config['password'],
            db=config['db']
        )
        tms_adapter = Timeseries(get_sessionmaker(engine=db_engine))

        for wrf_model in wrf_model_list:
            run_name = 'WRFv{}_{}'.format(wrf_version, wrf_model)
            rainc_net_cdf_file = 'RAINC_{}_{}.nc'.format(run_date_str, wrf_model)
            rainnc_net_cdf_file = 'RAINNC_{}_{}.nc'.format(run_date_str, wrf_model)
            rainc_net_cdf_file_path = os.path.join(output_dir, rainc_net_cdf_file)
            rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)
            station_prefix = 'wrf_v{}_{}'.format(wrf_version, wrf_model)
            try:
                read_netcdf_file(tms_adapter,
                                 rainc_net_cdf_file_path,
                                 rainnc_net_cdf_file_path,
                                 station_prefix, run_name)
            except Exception as e:
                print('Net CDF file reading error.')
                traceback.print_exc()
    except Exception as e:
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        current_time2 = datetime.datetime.now()
        print('---------------------------------------------------------------------')
        print('Data upload time : ', current_time2 - current_time1)
        print('---------------------------------------------------------------------')
