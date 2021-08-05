import logging
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd

from gtfs_router.utils import line_cutter

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("Initialize Logger")

# Default Projection used to calculate distances - Best for Continental US
ALBERS_EQUAL_AREA_CONICAL_EPSG = 5070

SHAPE_DIST_TRAVELED = "shape_dist_traveled"


def _get_trip_types(stop_times: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    trip_types = pd.concat(
        [
            stop_times.groupby("trip_id")["stop_id"].apply(lambda x: ",".join(list(x))),
            stop_times.groupby("trip_id")["stop_sequence"].apply(
                lambda x: ",".join(str(seq) for seq in x)
            ),
        ],
        axis=1,
        keys=["stop_id", "stop_sequence"],
    ).reset_index()

    trip_types = pd.merge(
        trip_types, trips[["trip_id", "route_id", "shape_id"]], on="trip_id"
    )
    return (
        trip_types.groupby(["route_id", "shape_id", "stop_id", "stop_sequence"])[
            "trip_id"
        ]
        .apply(lambda x: ",".join(list(x)))
        .reset_index()
    )


def _find_distances(
    trip_types: pd.DataFrame, shapes: gpd.GeoDataFrame, stops: gpd.GeoDataFrame
):
    _trip_types = trip_types.copy()
    _trip_types[SHAPE_DIST_TRAVELED] = None

    counter = 1
    total_rows = len(_trip_types)

    for trip_idx, trip_type in _trip_types.iterrows():
        if counter % 10 == 0:
            logger.info("Processing Unique Trip Group {} of {}".format(counter, total_rows))

        counter = counter + 1

        line = shapes[shapes["shape_id"] == trip_type["shape_id"]]["geometry"].values[0]

        trip_stops = trip_type["stop_id"].split(",")

        dist_traveled = []

        dist = 0

        for trip_stop in trip_stops:
            stop = stops[stops["stop_id"] == trip_stop]["geometry"].values[0]
            proj_dist = line.project(stop)
            _line = line_cutter(line, proj_dist)
            ## FIXME: This is a hack, some loops in routes cause problems
            if _line is None:
                logger.warning("Potential Error Calculating Distances")
                logger.warning("Idx Error: {}".format(trip_idx))
                logger.warning("Stop ID Error: {}".format(trip_stop))
            else:
                line = _line[-1]
            dist = proj_dist + dist

            dist_traveled.append(dist)

            if line is None:
                print("Idx Error: {}".format(trip_idx))
                print("Stop ID Error: {}".format(trip_type["stop_id"]))
                break

        trip_type[SHAPE_DIST_TRAVELED] = dist_traveled

    return _trip_types


def _expand_trip_types(trip_types: pd.DataFrame) -> pd.DataFrame:
    trip_dict = []

    for idx, trip_type in trip_types.iterrows():
        stop_ids = trip_type["stop_id"].split(",")
        stop_seq = trip_type["stop_sequence"].split(",")
        dist_traveled = trip_type[SHAPE_DIST_TRAVELED]

        assert len(stop_ids) == (len(dist_traveled))
        trip_dict.append(
            pd.DataFrame(
                data={
                    "route_id": [trip_type.route_id] * len(stop_ids),
                    "trip_id": [trip_type.trip_id] * len(stop_ids),
                    "stop_id": stop_ids,
                    "stop_sequence": stop_seq,
                    SHAPE_DIST_TRAVELED: dist_traveled,
                }
            )
        )

    trip_dict = pd.concat(trip_dict)

    trip_dict[["trip_id", "stop_id"]] = trip_dict[["trip_id", "stop_id"]].astype(str)
    trip_dict["stop_sequence"] = trip_dict["stop_sequence"].astype(int)

    return trip_dict


def _generate_new_trip_table(trip_types: pd.DataFrame) -> pd.DataFrame:
    trip_table = []

    for (route, trips), gb in trip_types.groupby(["route_id", "trip_id"]):
        num_trips = len(trips.split(","))

        trip_ids = np.repeat(trips.split(","), len(gb))

        route_ids = np.repeat(route, len(gb) * num_trips)

        stop_ids = np.tile(gb["stop_id"], num_trips)

        stop_seq = np.tile(gb["stop_sequence"], num_trips)

        dist_traveled = np.tile(gb[SHAPE_DIST_TRAVELED], num_trips)

        trip_table.append(
            pd.DataFrame(
                data={
                    "route_id": route_ids,
                    "trip_id": trip_ids,
                    "stop_id": stop_ids,
                    "stop_sequence": stop_seq,
                    SHAPE_DIST_TRAVELED: dist_traveled,
                }
            )
        )

    return pd.concat(trip_table)


def generate_shape_dist_traveled(
    gtfs_feed: pd.DataFrame,
    epsg: Optional[int] = ALBERS_EQUAL_AREA_CONICAL_EPSG,
    overwrite: Optional[bool] = False,
) -> pd.DataFrame:
    """

    :param gtfs_feed: A Partridge GTFS datafeed
    :param overwrite:
    :return:
    """
    shapes = gtfs_feed.shapes.to_crs(epsg=epsg)
    stops = gtfs_feed.stops.to_crs(epsg=epsg)
    stop_times = gtfs_feed.stop_times.sort_values(["trip_id", "stop_sequence"])
    trips = gtfs_feed.trips.copy()

    if (
        SHAPE_DIST_TRAVELED in shapes.columns
        or SHAPE_DIST_TRAVELED in stop_times.columns
    ) and not overwrite:
        logger.warning(
            "'shape_dist_traveled' already exists. Use overwrite argument to overwrite values"
        )
        return

    trip_types = _get_trip_types(stop_times, trips)
    trip_types = _find_distances(trip_types, shapes, stops)
    trip_types = _expand_trip_types(trip_types)
    trip_table = _generate_new_trip_table(trip_types)

    assert len(trip_table) == len(stop_times)

    return pd.merge(
        stop_times,
        trip_table[["trip_id", "stop_id", "stop_sequence", SHAPE_DIST_TRAVELED]],
        on=["trip_id", "stop_id", "stop_sequence"],
    )
