import logging

import partridge as ptg

logger = logging.getLogger()


def log_stop_information(partridge_feed, stop_id):
    stops = partridge_feed.stops
    stop = stops[stops["stop_id"] == stop_id]
    if stop.empty:
        logger.waring("No stop_id found for: {}".format(stop_id))
    stop = stop.head(1).squeeze()
    logger.debug("-------STOP INFO------")
    logger.debug("stop_id: {}".format(stop_id))
    logger.debug("stop_name: {}".format(stop["stop_name"]))
    logger.debug(
        "stop_coordinates: {:.3f}, {:.3f}".format(
            stop["geometry"].x, stop["geometry"].y
        )
    )
    stop_times = partridge_feed.stop_times
    trip_ids = stop_times[stop_times["stop_id"] == stop_id]["trip_id"].unique()
    route_ids = partridge_feed.trips[partridge_feed.trips["trip_id"].isin(trip_ids)][
        "route_id"
    ].unique()
    routes = partridge_feed.routes[partridge_feed.routes["route_id"].isin(route_ids)]
    logger.debug("Routes:")
    for route_id, short_name, long_name in routes[
        ["route_id", "route_short_name", "route_long_name"]
    ].itertuples(index=False, name=None):
        logger.debug("\tRoute ID: {}:\t{}-{}".format(route_id, short_name, long_name))
    logger.debug("Trips: {}".format(trip_ids))
