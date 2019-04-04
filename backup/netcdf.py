import shutil
import traceback
from random import random
from netCDF4 import Dataset
import numpy as np
import os
import json
import gzip
from datetime import datetime, timedelta
from pip.utils import logging
from curwmysqladapter import Station, Data
import pandas as pd
SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]
# [lon_min, lat_min, lon_max, lat_max] : [79.5214614868164, 5.722969055175781, 82.1899185180664, 10.064254760742188]


def push_rainfall_to_db(timeseries_dict, types=None, timesteps=24, upsert=False, source='WRF',
                        source_params='{}', name='WRFv3_A'):
    for station, timeseries in timeseries_dict.items():
        print('Pushing data for station ' + station)
        meta_data = {
            'sim_tag': '',
			'scheduled_date': '',
			'latitude': '',
			'longitude': '',
			'model': '',
			'version': '',
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


def read_netcdf_file(rainc_net_cdf_file_path, rainnc_net_cdf_file_path, station_prefix,
                     run_name, upsert=False):
    print('rainc_net_cdf_file_path : ', rainc_net_cdf_file_path)
    print('rainnc_net_cdf_file_path : ', rainnc_net_cdf_file_path)
    print('station_prefix : ', station_prefix)
    if not os.path.exists(rainc_net_cdf_file_path):
        print('no rainc netcdf')
    elif not os.path.exists(rainnc_net_cdf_file_path):
        print('no rainnc netcdf')
    else:
        nc_fid = Dataset(rainc_net_cdf_file_path, mode='r')
        rainc_unit_info = nc_fid.variables['RAINC'].units
        print('rainc_unit_info: ', rainc_unit_info)
        lat_unit_info = nc_fid.variables['XLAT'].units
        print('lat_unit_info: ', lat_unit_info)
        time_unit_info = nc_fid.variables['XTIME'].units
        print('time_unit_info: ', time_unit_info)

        lats = nc_fid.variables['XLAT'][0, :, 0]
        lons = nc_fid.variables['XLONG'][0, 0, :]


        time_unit_info_list = time_unit_info.split(' ')

        # lon_min, lat_min, lon_max, lat_max = SRI_LANKA_EXTENT
        lon_min = lons[0].item()
        lat_min = lats[0].item()
        lon_max = lons[-1].item()
        lat_max = lats[-1].item()
        print('[lon_min, lon_max, lat_min, lat_max] :', [lon_min, lon_max, lat_min, lat_max])

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

        def random_check_stations_exist():
            # for _ in range(10):
            #     _x = lons[int(random() * width)]
            #     _y = lats[int(random() * height)]
            #     _name = '%s_%.6f_%.6f' % (station_prefix, _x, _y)
            #     _query = {'name': _name}
            #     if curw_db_adapter.get_station(_query) is None:
            #         logging.debug('Random stations check fail')
            #         return False
            # logging.debug('Random stations check success')
            return True

        stations_exists = random_check_stations_exist()
        rf_ts = {}
        for y in range(height):
            for x in range(width):
                lat = lats[y]
                lon = lons[x]

                station_id = '%s_%.6f_%.6f' % (station_prefix, lon, lat)
                name = station_id

                if not stations_exists:
                    logging.info('Creating station %s ...' % name)
                    station = [Station.WRF, station_id, name, str(lon), str(lat), str(0), "WRF point"]
                    # curw_db_adapter.create_station(station)

                # add rf series to the dict
                ts = []
                for i in range(len(diff)):
                    ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S') + timedelta(
                        minutes=times[i].item())
                    t = datetime_utc_to_lk(ts_time, shift_mins=30)
                    ts.append([t.strftime('%Y-%m-%d %H:%M:%S'), diff[i, y, x]])
                data_frame = pd.DataFrame(ts, columns=['time', 'value']).set_index(keys='time')
                rf_ts[name] = data_frame
                #print(data_frame)
                #exit(0)
        #print(rf_ts)
        push_rainfall_to_db(rf_ts, source=station_prefix, upsert=upsert, name=run_name)


if __name__ == "__main__":
    try:
        config = json.loads(open('/home/hasitha/PycharmProjects/Wrf/configs/config.json').read())
        wrf_dir = '/mnt/disks/wrf-mod'
        wrf_version = '3'
        wrf_model_list = 'A,C,E,SE'
        if 'wrf_dir' in config:
            wrf_dir = config['wrf_dir']
        if 'wrf_version' in config:
            wrf_version = config['wrf_version']
        if 'wrf_model_list' in config:
            wrf_model_list = config['wrf_model_list']
        wrf_model_list = wrf_model_list.split(',')

        #run_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        run_date_str = '2019-03-21'
        daily_dir = 'STATIONS_{}'.format(run_date_str)

        #output_dir = os.path.join(wrf_dir, daily_dir)
        output_dir = '/home/hasitha/PycharmProjects/Wrf/wrf_output'

        for wrf_model in wrf_model_list:
            run_name = 'WRFv{}_{}'.format(wrf_version, wrf_model)
            rainc_net_cdf_file = 'RAINC_{}_{}.nc'.format(run_date_str, wrf_model)
            rainnc_net_cdf_file = 'RAINNC_{}_{}.nc'.format(run_date_str, wrf_model)
            rainc_net_cdf_file_path = os.path.join(output_dir, rainc_net_cdf_file)
            rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)
            station_prefix = 'wrf_v{}_{}'.format(wrf_version, wrf_model)
            try:
                read_netcdf_file(rainc_net_cdf_file_path, rainnc_net_cdf_file_path,
                                 station_prefix, run_name)
            except Exception as e:
                print('Net CDF file reading error.')
                traceback.print_exc()
    except Exception as e:
        print('JSON config data loading error.')
        traceback.print_exc()
