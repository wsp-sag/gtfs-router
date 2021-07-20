import logging
import os
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

OVERWRITE = 'overwrite'
APPEND = 'append'
NO_OVERWRITE = 'no overwrite'

# Default Projection used to calculate distances - Best for Continental US
ALBERS_EQUAL_AREA_CONICAL_EPSG = 5070

# Default buffer distance to search for transfers - If used with
# default projection, this will be in meters. 150 meters is approximately 500 feet.
DEFAULT_BUFFER_DISTANCE = 150

# Default walk speed. Represented as <projection units> / minute
# In the defaults setup, the walk speed is 55 meters / minute (slightly faster than 2mph)
DEFAULT_WALK_SPEED = 55

TRANSFER_HEADERS = ['from_stop_id', 'to_stop_id', 'transfer_type', 'min_transfer_time']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug('Initialize Logger')


def find_transfers(stops: pd.DataFrame, distance: Optional[float] = DEFAULT_BUFFER_DISTANCE,
                   epsg: Optional[float] = ALBERS_EQUAL_AREA_CONICAL_EPSG,
                   walk_speed: Optional[float] = DEFAULT_WALK_SPEED,
                   ) -> pd.DataFrame:

    # If this is not a GeoDataFrame, create one, so we project to a cartesian system for easier distances
    # TODO: Explore just doing all distances with haversine formula in polar coordinates
    if not isinstance(stops, gpd.GeoDataFrame):
        stops = gpd.GeoDataFrame(data=stops, index=stops.index,
                                 geometry=[Point(xy) for xy in zip(stops['stop_lon'], stops['stop_lat'])],
                                 crs='epsg:4326')

    # Project to cartesian system
    stops = stops.to_crs(epsg=epsg)
    stops['x'] = stops['geometry'].x
    stops['y'] = stops['geometry'].y

    # Grab a copy of the stop_id, x, y
    buffers = stops[['stop_id', 'x', 'y']].values

    # Build a dataframe that will effectively become the cross join
    buffers = pd.DataFrame(index=np.repeat(stops.index, len(stops.index)),
                           data=np.tile(buffers, (len(stops.index), 1)),
                           columns=['stop_id', 'x', 'y']
                           )

    # "Cross" Join the data
    buffers = pd.merge(stops[['stop_id', 'geometry', 'x', 'y']], buffers,
                       left_index=True, right_index=True, suffixes=['', '_potential'])

    # Figure out the euclidean distance between all the points
    buffers['dist'] = np.sqrt((np.power(buffers['x'] - buffers['x_potential'], 2) +
                               np.power(buffers['y'] - buffers['y_potential'], 2)).astype(float))

    # Filter where the from = to and distances are greater than the threshold
    stop_id_filter = buffers['stop_id'] != buffers['stop_id_potential']
    distance_filter = buffers['dist'] <= distance
    buffers = buffers[distance_filter & stop_id_filter].copy()

    # Calculate the walk time in seconds
    buffers['walk_time'] = (buffers['dist'] / walk_speed) * 60

    # Retain the stuff needed
    buffers = buffers[['stop_id', 'stop_id_potential', 'walk_time']].copy()
    buffers = buffers.rename(columns={'stop_id': 'from_stop_id', 'stop_id_potential': 'to_stop_id',
                                      'walk_time': 'min_transfer_time'})

    # Add the transfer_type from the GTFS Specification
    buffers['transfer_type'] = 2

    # Return the dataframe consistent with the GTFS Specification
    return buffers[TRANSFER_HEADERS]


def update_transfers(gtfs_path: str,
                     write_type: Optional[str] = NO_OVERWRITE,
                     distance: Optional[float] = DEFAULT_BUFFER_DISTANCE,
                     epsg: Optional[float] = ALBERS_EQUAL_AREA_CONICAL_EPSG,
                     walk_speed: Optional[float] = DEFAULT_WALK_SPEED,):

    # Check to see if the transfer.txt file already exists
    xfer_file_exists = os.path.exists(os.path.join(gtfs_path, 'transfers.txt'))

    # If not overwrite and file exist, give tell the user that it can't overwrite
    if write_type == NO_OVERWRITE:
        if xfer_file_exists:
            err = 'transfer.txt already exists at {}. Please specify overwrite or append.'.format(gtfs_path)
            logger.fatal(err)
            raise IOError(err)

    # if the (append or overwrite) and transfers.txt doesn't exist, give the user a heads up.
    if write_type in [APPEND, OVERWRITE]:
        if not xfer_file_exists:
            logger.warning('transfer.txt does not exist at {}. File will be created'.format(gtfs_path))

    # Read in the stops
    stops = pd.read_csv(os.path.join(gtfs_path, 'stops.txt'))

    # Get all possible transfers between stops within threshold distance
    transfers = find_transfers(stops, distance, epsg, walk_speed)

    # if append, read in the existing file and add new records
    if xfer_file_exists and write_type == APPEND:
        existing_transfers = pd.read_csv(os.path.join(gtfs_path, 'transfers.txt'))

        # Make sure the optional fields are available.
        for optional_col in ['transfer_type', 'min_transfer_time']:
            if optional_col not in existing_transfers.columns:
                existing_transfers[optional_col] = np.nan

        transfers = pd.concat(existing_transfers[TRANSFER_HEADERS], transfers[TRANSFER_HEADERS])
        transfers = transfers.drop_duplicates(subset=['from_stop_id', 'to_stop_id'], keep='first')

    # Cast variables just to make sure
    transfers[['to_stop_id', 'from_stop_id']] = transfers[['to_stop_id', 'from_stop_id']].astype(int).astype(str)

    # Export to disk
    transfers.to_csv(os.path.join(gtfs_path, 'transfers.txt'), index=False, float_format='%.1f')


if __name__ == '__main__':
    update_transfers(os.path.join('data', 'sacramento_2021_03_15'), write_type=OVERWRITE)
