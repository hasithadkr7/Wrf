import logging
import ntpath
import os
import glob
import shutil
from zipfile import ZipFile
import numpy as np
from netCDF4._netCDF4 import Dataset
import datetime as dt
from configs.constants import ZIP_DEFLATED


def extract_variables(nc_f, var_list, lat_min, lat_max, lon_min, lon_max, lat_var='XLAT', lon_var='XLONG',
                      time_var='Times'):
    """
    extract variables from a netcdf file
    :param nc_f:
    :param var_list: comma separated string for variables / list of strings
    :param lat_min:
    :param lat_max:
    :param lon_min:
    :param lon_max:
    :param lat_var:
    :param lon_var:
    :param time_var:
    :return:
    variables dict {var_key --> var[time, lat, lon], xlat --> [lat], xlong --> [lon], times --> [time]}
    """
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables[time_var][:]])
    lats = nc_fid.variables[lat_var][0, :, 0]
    lons = nc_fid.variables[lon_var][0, 0, :]

    lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
    lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

    vars_dict = {}
    if isinstance(var_list, str):
        var_list = var_list.replace(',', ' ').split()
    # var_list = var_list.replace(',', ' ').split() if isinstance(var_list, str) else var_list
    for var in var_list:
        vars_dict[var] = nc_fid.variables[var][:, lat_inds[0], lon_inds[0]]

    nc_fid.close()

    vars_dict[time_var] = times
    vars_dict[lat_var] = lats[lat_inds[0]]
    vars_dict[lon_var] = lons[lon_inds[0]]

    # todo: implement this archiving procedure
    # if output is not None:
    #     logging.info('%s will be archied to %s' % (nc_f, output))
    #     ncks_extract_variables(nc_f, var_str, output)

    return vars_dict


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_mean_cell_size(lats, lons):
    return np.round(np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)]
                                      - lats[0: len(lats) - 1])), 3)


def datetime_to_epoch(timestamp=None):
    timestamp = dt.datetime.now() if timestamp is None else timestamp
    return (timestamp - dt.datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return dt.datetime(1970, 1, 1) + dt.timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(np.math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def datetime_lk_to_utc(timestamp_lk):
    return timestamp_lk - dt.timedelta(hours=5, minutes=30)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + dt.timedelta(hours=5, minutes=30 + shift_mins)


def file_exists_nonempty(filename):
    return os.path.exists(filename) and os.path.isfile(filename) and os.stat(filename).st_size != 0


def create_asc_file(data, lats, lons, out_file_path, cell_size=0.1, no_data_val=-99, overwrite=False):
    if not file_exists_nonempty(out_file_path) or overwrite:
        with open(out_file_path, 'wb') as out_file:
            out_file.write(('NCOLS %d\n' % len(lons)).encode())
            out_file.write(('NROWS %d\n' % len(lats)).encode())
            out_file.write(('XLLCORNER %f\n' % lons[0]).encode())
            out_file.write(('YLLCORNER %f\n' % lats[0]).encode())
            out_file.write(('CELLSIZE %f\n' % cell_size).encode())
            out_file.write(('NODATA_VALUE %d\n' % no_data_val).encode())

            np.savetxt(out_file, data, fmt='%.4f')
    else:
        logging.info('%s already exits' % out_file_path)


def copy_if_not_exists(src, dest):
    if not file_exists_nonempty(dest):
        return shutil.copy2(src, dest)
    else:
        return dest


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def create_symlink_with_prefix(src_dir, prefix, dest_dir):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.symlink(filename, os.path.join(dest_dir, ntpath.basename(filename)))


# def create_zipfile(file_list, output, compression=ZIP_DEFLATED):
#     with ZipFile(output, 'w', compression=compression) as z:
#         for file in file_list:
#             z.write(file)


def create_zip_with_prefix(src_dir, regex, dest_zip, comp=ZIP_DEFLATED, clean_up=False):
    with ZipFile(dest_zip, 'w', compression=comp) as zip_file:
        for filename in glob.glob(os.path.join(src_dir, regex)):
            zip_file.write(filename, arcname=os.path.basename(filename))
            if clean_up:
                os.remove(filename)
    return dest_zip


def push_rainfall_to_db(curw_db_adapter, timeseries_dict, types=None, timesteps=24, upsert=False, source='WRF',
                        source_params='{}', name='Cloud-1'):
    if types is None:
        types = ['Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after']

    if not curw_db_adapter.get_source(name=source):
        logging.info('Creating source ' + source)
        curw_db_adapter.create_source([source, source_params])

    for station, timeseries in timeseries_dict.items():
        logging.info('Pushing data for station ' + station)
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
                logging.debug('HASH SHA256 created: ' + event_id)

            row_count = curw_db_adapter.insert_timeseries(event_id, timeseries[i * timesteps:(i + 1) * timesteps],
                                                          upsert=upsert)
            logging.info('%d rows inserted' % row_count)


def extract_time_data(nc_f):
    nc_fid = Dataset(nc_f, 'r')
    times_len = len(nc_fid.dimensions['Time'])
    try:
        times = [''.join(x) for x in nc_fid.variables['Times'][0:times_len]]
    except TypeError:
        times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables['Times'][:]])
    nc_fid.close()
    return times_len, times


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


