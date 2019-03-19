from netCDF4 import Dataset
import numpy as np
import os
from datetime import datetime, timedelta
from utils import utils as ext_utils


def push_rainfall_to_db(curw_db_adapter, timeseries_dict, types=None, timesteps=24, upsert=False, source='WRF',
                        source_params='{}', name='Cloud-1'):
    if types is None:
        types = ['Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after']

    if not curw_db_adapter.get_source(name=source):
        print('Creating source ' + source)
        curw_db_adapter.create_source([source, source_params])

    for station, timeseries in timeseries_dict.items():
        print('Pushing data for station ' + station)
        for i in range(int(np.ceil(len(timeseries) / timesteps))):
            meta_data = {
                'station': station,
                'variable': 'Precipitation',
                'unit': 'mm',
                'type': types[i],
                'source': source,
                'name': name,
            }

            event_id = curw_db_adapter.get_event_id(meta_data)
            if event_id is None:
                event_id = curw_db_adapter.create_event_id(meta_data)
                print('HASH SHA256 created: ' + event_id)

            row_count = curw_db_adapter.insert_timeseries(event_id, timeseries[i * timesteps:(i + 1) * timesteps],
                                                          upsert=upsert)
            print('%d rows inserted' % row_count)


def read_netcdf_file(rainc_net_cdf_file_path, rainnc_net_cdf_file_path, station_prefix):
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

        # lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT
        lon_min = lons[0].item()
        lat_min = lats[0].item()
        lon_max = lons[-1].item()
        lat_max = lats[-1].item()
        lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
        lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

        rainc = nc_fid.variables['RAINC'][:, lat_inds[0], lon_inds[0]]

        nnc_fid = Dataset(rainnc_net_cdf_file_path, mode='r')
        rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

        rain = rainc+rainnc

        times = nc_fid.variables['XTIME'][:]

        time_unit_info_list = time_unit_info.split(' ')

        nc_fid.close()
        nnc_fid.close()

        diff = ext_utils.get_two_element_average(rain)

        width = len(lons)
        height = len(lats)
        rf_ts = {}
        for y in range(height):
            for x in range(width):
                lat = lats[y]
                lon = lons[x]
                station_id = '%s_%.6f_%.6f' % (station_prefix, lon, lat)
                name = station_id
                print('name : ', name)
                ts = []
                for i in range(len(diff)):
                    ts_time = datetime.strptime(time_unit_info_list[2], '%Y-%m-%dT%H:%M:%S')+timedelta(minutes=times[i].item())
                    t = ext_utils.datetime_utc_to_lk(ts_time, shift_mins=30)
                    ts.append([t.strftime('%Y-%m-%d %H:%M:%S'), diff[i, y, x]])
                rf_ts[name] = ts
                print('ts : ', ts)


if __name__ == "__main__":
    date_str = '2019-03-17'
    wrf_dir = '/mnt/disks/wrf-mod'
    output_dir = '/home/hasitha/PycharmProjects/Wrf/wrf_output'
    #output_dir = 'STATIONS_{}'.format(date_str)
    wrf_version = '3'
    wrf_model = 'A'

    run_name = 'WRFv{}_{}'.format(wrf_version, wrf_model)

    rainc_net_cdf_file = 'RAINC_{}_{}.nc'.format(date_str, wrf_model)
    rainnc_net_cdf_file = 'RAINNC_{}_{}.nc'.format(date_str, wrf_model)

    # rainc_net_cdf_file_path = os.path.join(wrf_dir, output_dir, rainc_net_cdf_file)
    # rainnc_net_cdf_file_path = os.path.join(wrf_dir, output_dir, rainnc_net_cdf_file)

    rainc_net_cdf_file_path = os.path.join(output_dir, rainc_net_cdf_file)
    rainnc_net_cdf_file_path = os.path.join(output_dir, rainnc_net_cdf_file)

    station_prefix = 'wrf_v{}_{}'.format(wrf_version, wrf_model)
    net_cdf_file_path = os.path.join(output_dir, rainc_net_cdf_file)

    read_netcdf_file(net_cdf_file_path, rainnc_net_cdf_file_path, station_prefix)

