import logging
import time
from typing import List, Optional, Union

import geopandas as gpd
import pandas as pd
import pyproj
from shapely.geometry import LineString
from shapely.ops import transform

from gtfs_router import ALBERS_EQUAL_AREA_CONICAL_EPSG
from gtfs_router.utils import line_cutter

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("tests")


class StopAccessState:
    def __init__(self, origin_stop_id: str, gtfs_feed):
        """State tracker for stop ids."""
        self._stops = {}

        # initialize the origin node with no prior trip history
        self._origin = origin_stop_id
        self._gtfs_feed = gtfs_feed
        self.try_add_update(self._origin, 0)

    def all_stops(self):
        return list(self._stops.keys())

    def has_stop(self, stop_id: str):
        return stop_id in self._stops.keys()

    def get_stop(self, stop_id: str):
        return self._stops[stop_id]

    def get_stops(self, stop_ids: List[str]):
        return {stop_id: self._stops[stop_id] for stop_id in stop_ids}

    def get_preceding_trips(self, stop_ids: List[str]):
        return [
            (stop_id, preceding)
            for stop_id in stop_ids
            for preceding in self._stops[stop_id]["preceding"]
        ]

    def try_add_update(
        self,
        stop_id: str,
        time_to_reach: Union[int, float],
        trip_id: Optional[str] = None,
        preceding_stop: Optional[str] = None,
        preceding_path: Optional[List[str]] = None,
        k: Optional[int] = None,
    ) -> bool:
        # initialize return object
        did_update = False

        if stop_id in self._stops.keys():
            if self._stops[stop_id]["time_to_reach"] > time_to_reach:
                # update the stop access attributes
                self._stops[stop_id]["time_to_reach"] = time_to_reach
                did_update = True
        else:
            self._stops[stop_id] = {
                "time_to_reach": time_to_reach,
                "preceding": [trip_id] if trip_id else [],
                "trip_path": {},
                "stop_path": {},
            }
            did_update = True

        if did_update:
            if not k is None:
                self._stops[stop_id]["prior_segment"] = {
                    "segment_num": k,
                    "trip_id": trip_id if k % 2 == 0 else "walk transfer",
                    "from_stop_id": preceding_stop,
                }

            # override if a preceding path is provided
            if preceding_path:
                self._stops[stop_id]["preceding"] = preceding_path.copy()

            # add current trip id to the path of trips taken, avoiding dupes
            if trip_id is not None:
                if len(self._stops[stop_id]["preceding"]) == 0:
                    self._stops[stop_id]["preceding"] = [trip_id]
                elif trip_id != self._stops[stop_id]["preceding"][-1]:
                    self._stops[stop_id]["preceding"] = self._stops[stop_id][
                        "preceding"
                    ].copy() + [trip_id]

        return did_update

    def _get_trip_segment(
        self,
        prior_stop_mp,
        current_stop_mp,
        current_trip_id,
        epsg=ALBERS_EQUAL_AREA_CONICAL_EPSG,
    ):
        stops = self._gtfs_feed.stops
        # prior_stop = stops[stops['stop_id'] == prior_stop_id].values[0]
        # current_stop = stops[stops['stop_id'] == current_stop_id].values[0]

        trips = self._gtfs_feed.trips
        shape_id = trips[trips["trip_id"] == current_trip_id]["shape_id"].values[0]

        shapes = self._gtfs_feed.shapes.to_crs(epsg=epsg)
        route_shape = shapes[shapes["shape_id"] == shape_id]["geometry"].values[0]

        line = line_cutter(route_shape, prior_stop_mp)[-1]
        line = line_cutter(line, current_stop_mp - prior_stop_mp)[0]

        from_proj = pyproj.CRS("EPSG:5070")
        to_proj = pyproj.CRS("EPSG:4326")

        project = pyproj.Transformer.from_crs(
            from_proj, to_proj, always_xy=True
        ).transform
        return transform(project, line)

    def describe_path(
        self, to_stop_id: str, epsg: Optional[int] = ALBERS_EQUAL_AREA_CONICAL_EPSG
    ) -> List[str]:
        stops = self._gtfs_feed.stops
        stop_times = self._gtfs_feed.stop_times
        trips = self._gtfs_feed.trips
        routes = self._gtfs_feed.routes

        current_stop_id = to_stop_id
        current_stop = self.get_stop(to_stop_id)
        max_segment_num = current_stop["prior_segment"]["segment_num"]

        out_messages = {}
        segments = {}
        from_stop = {}
        from_name = {}
        from_stop_lat = {}
        from_stop_lon = {}
        to_stop = {}
        to_name = {}
        to_stop_lat = {}
        to_stop_lon = {}
        mode = {}
        color = {}

        for x in range(max_segment_num, -1, -1):
            if "prior_segment" not in current_stop or current_stop["prior_segment"]["segment_num"] != x:
                continue

            prior_stop_id = current_stop["prior_segment"]["from_stop_id"]
            prior_stop = self.get_stop(prior_stop_id)

            current_stop_row = stops[stops["stop_id"] == current_stop_id].iloc[0].squeeze()
            prior_stop_row = stops[stops["stop_id"] == prior_stop_id].iloc[0].squeeze()

            current_stop_name = current_stop_row["stop_name"]
            prior_stop_name = prior_stop_row["stop_name"]

            current_trip_id = current_stop["prior_segment"]["trip_id"]

            from_stop[x] = prior_stop_id
            to_stop[x] = current_stop_id

            from_name[x] = prior_stop_name
            from_stop_lat[x] = prior_stop_row['geometry'].y
            from_stop_lon[x] = prior_stop_row['geometry'].x

            to_name[x] = current_stop_name
            to_stop_lat[x] = current_stop_row['geometry'].y
            to_stop_lon[x] = current_stop_row['geometry'].x

            if current_trip_id != "walk transfer":
                route_id = trips[trips["trip_id"] == current_trip_id]["route_id"]
                route_color = routes[routes["route_id"].isin(route_id)][
                    "route_color"
                ].values[0]

                color[x] = "#" + route_color
                route_name = (
                    routes[routes["route_id"].isin(route_id)][
                        "route_short_name"
                    ].values[0]
                    + "-"
                    + routes[routes["route_id"].isin(route_id)][
                        "route_long_name"
                    ].values[0]
                )

                boarding_stop_time = stop_times[
                    (stop_times["trip_id"] == current_trip_id)
                    & (stop_times["stop_id"] == prior_stop_id)
                ]

                boarding_time = boarding_stop_time["departure_time"].values[0]


                alight_stop_time = stop_times[
                    (stop_times["trip_id"] == current_trip_id)
                    & (stop_times["stop_id"] == current_stop_id)
                ]

                alight_time = alight_stop_time["arrival_time"].values[0]



                segments[x] = self._get_trip_segment(
                    boarding_stop_time["shape_dist_traveled"].values[0],
                    alight_stop_time["shape_dist_traveled"].values[0],
                    current_trip_id,
                    epsg=epsg,
                )
                mode[x] = "transit"

                out_messages[x] = (
                    "Board {route_name} at {prior_stop_name}({prior_stop_id}) at {boarding_time} -> "
                    "Alight at {current_stop_name}({current_stop_id}) at {alight_time}".format(
                        route_name=route_name,
                        prior_stop_name=prior_stop_name,
                        prior_stop_id=prior_stop_id,
                        boarding_time=StopAccessState._format_time(boarding_time),
                        current_stop_name=current_stop_name,
                        current_stop_id=current_stop_id,
                        alight_time=StopAccessState._format_time(alight_time),
                    )
                )
            else:  # Walk connection
                mode[x] = "walk"
                color[x] = "#000000"
                current_stop_geom = stops[stops["stop_id"] == current_stop_id][
                    "geometry"
                ].values[0]
                prior_stop_geom = stops[stops["stop_id"] == prior_stop_id][
                    "geometry"
                ].values[0]

                segments[x] = LineString([prior_stop_geom, current_stop_geom])
                out_messages[x] = "Walk from {}({}) to {}({})".format(
                    prior_stop_name,
                    prior_stop_id,
                    current_stop_name,
                    current_stop_id,
                    current_trip_id,
                )

            current_stop = prior_stop
            current_stop_id = prior_stop_id

        # counter = 1
        # sorted_messages = {}
        # sorted_segments = {}
        # for i in sorted(list(out_messages.keys())):
        #    sorted_messages[counter] = out_messages[i]
        #    if i in segments.keys():
        #        sorted_segments[counter] = segments[i]
        #    counter = counter + 1
        return gpd.GeoDataFrame(
            index=segments.keys(),
            data={
                "from_stop_id": from_stop.values(),
                "to_stop_id": to_stop.values(),
                "from_stop_name": from_name.values(),
                "to_stop_name": to_name.values(),
                "from_stop_lat": from_stop_lat.values(),
                "from_stop_lon": from_stop_lon.values(),
                "to_stop_lat": to_stop_lat.values(),
                "to_stop_lon": to_stop_lon.values(),
                "desc": out_messages.values(),
                "mode": mode.values(),
                "color": color.values(),
            },
            geometry=list(segments.values()),
            crs="epsg:4326",
        )

        # return sorted_messages, segments

    @staticmethod
    def _format_time(seconds: float) -> str:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = (seconds % 3600) % 60

        return "{:02.0f}:{:02.0f}:{:02.0f}".format(hours, minutes, seconds)


def _get_trip_ids_for_stop_new(
    stop_times: pd.DataFrame, stop_ids: List[str], departure_time: int
):
    """Takes a stop and departure time and get associated trip ids."""
    mask_1 = stop_times.stop_id.isin(stop_ids)
    # mask_2 = stop_times.departure_time >= departure_time
    # mask_3 = stop_times.departure_time <= departure_time + 120 * 60

    # extract the list of qualifying trip ids
    potential_trips = stop_times[mask_1][
        ["trip_id", "stop_id", "departure_time", "arrival_time", "stop_sequence"]
    ].drop_duplicates()
    return potential_trips


def _remove_prior_trips(potential_trips: pd.DataFrame, prior_trips: List[tuple]):
    tuples_in_df = pd.MultiIndex.from_frame(potential_trips[["stop_id", "trip_id"]])
    return potential_trips[~tuples_in_df.isin(prior_trips)].copy()


def _stop_times_for_kth_trip(
    stops_state: StopAccessState,
    last_updated_stops: List[str],
    stop_times: pd.DataFrame,
    departure_time: float,
    k: int,
) -> None:
    tic = time.perf_counter()
    # find all trips already related to these stop
    prior_trips = stops_state.get_preceding_trips(last_updated_stops)

    # find all qualifying potential trips with these stops
    potential_trips = _get_trip_ids_for_stop_new(
        stop_times, last_updated_stops, departure_time
    )

    if prior_trips:
        potential_trips = _remove_prior_trips(potential_trips, prior_trips)

    # trip_stop_pairings = potential_trips.groupby('trip_id')['stop_id'].apply(list).to_dict()
    toc = time.perf_counter()
    logger.debug("\t\tTrip Pairings calculated in {:0.4f} seconds".format(toc - tic))

    # This is a dead end...
    if potential_trips.empty:
        return False

    tic = time.perf_counter()

    last_stop_evaluated = potential_trips.loc[
        potential_trips.groupby("trip_id")["arrival_time"].idxmax()
    ]
    last_stop_states = stops_state.get_stops(
        list(last_stop_evaluated["stop_id"].unique())
    )
    last_stop_states = pd.DataFrame(
        index=last_stop_states.keys(), data=last_stop_states.values()
    )
    last_stop_evaluated = pd.merge(
        last_stop_evaluated,
        last_stop_states,
        left_on="stop_id",
        right_index=True,
        how="left",
    )

    last_stop_evaluated = pd.merge(
        stop_times, last_stop_evaluated, on="trip_id", suffixes=["", "_preceding"]
    )
    # Only want to consider what happens after the stop in question
    filter1 = (
        last_stop_evaluated["stop_sequence"]
        >= last_stop_evaluated["stop_sequence_preceding"]
    )
    # A traveler can only use trips that leave after they arrive at the station
    filter2 = (
        last_stop_evaluated["departure_time_preceding"]
        >= last_stop_evaluated["time_to_reach"] + departure_time
    )

    last_stop_evaluated = last_stop_evaluated[filter1 & filter2].copy()

    last_stop_evaluated["arrive_time_adjusted"] = (
        last_stop_evaluated["arrival_time"] - departure_time
    )  # + last_stop_evaluated['time_to_reach']

    # routes_evaluated = trips[trips['trip_id'].isin(last_stop_evaluated['trip_id'].unique())]
    # logger.debug('Routes Evaluated: {}'.format(routes_evaluated['route_id'].unique()))

    for (
        arrive_stop_id,
        arrive_time_adjusted,
        trip_id,
        preceding_stop,
        preceding_path,
    ) in last_stop_evaluated[
        ["stop_id", "arrive_time_adjusted", "trip_id", "stop_id_preceding", "preceding"]
    ].itertuples(
        index=False, name=None
    ):

        stops_state.try_add_update(
            arrive_stop_id,
            arrive_time_adjusted,
            trip_id,
            preceding_stop,
            preceding_path,
            k * 2,
        )

    toc = time.perf_counter()
    logger.debug(
        "\t\t'Iterate' New Trip Pairings calculated in {:0.4f} seconds".format(
            toc - tic
        )
    )

    return True


def _add_footpath_transfers(
    stops_state: StopAccessState,
    transfers: pd.DataFrame,
    already_processed_stops: List[str],
    k,
) -> List[str]:
    # initialize a return object
    updated_stop_ids = []

    # add in transfers to nearby stops
    stop_ids = stops_state.all_stops()

    stop_xfers = transfers[
        (transfers["from_stop_id"].isin(stop_ids))
        & (~transfers["from_stop_id"].isin(already_processed_stops))
    ].copy()

    # No transfer from the stops
    if stop_xfers.empty:
        return updated_stop_ids

    ref_stop_state = [
        {
            "from_stop_id": stop_id,
            "time_to_reach": vals["time_to_reach"],
            "preceding": vals["preceding"],
        }
        for stop_id, vals in stops_state._stops.items()
    ]

    ref_stop_state = pd.DataFrame(ref_stop_state)

    stop_xfers = pd.merge(stop_xfers, ref_stop_state, on="from_stop_id", how="left")

    stop_xfers["arrive_time_adjusted"] = (
        stop_xfers["time_to_reach"] + stop_xfers["min_transfer_time"]
    )
    stop_xfers = stop_xfers.loc[
        stop_xfers.groupby("to_stop_id")["arrive_time_adjusted"].idxmin()
    ]
    stop_xfers["last_trip_id"] = stop_xfers.apply(
        lambda x: x["preceding"][-1] if len(x["preceding"]) else "", axis=1
    )

    for i, row in stop_xfers.iterrows():
        did_update = stops_state.try_add_update(
            row["to_stop_id"],
            row["arrive_time_adjusted"],
            row["last_trip_id"],
            preceding_stop=row["from_stop_id"],
            k=k * 2 + 1,
        )

        if did_update:
            updated_stop_ids.append(row["to_stop_id"])

    return updated_stop_ids


def raptor_assignment(
    feed, from_stop_id, to_stop_id, departure_time, transfers, transfer_limit
) -> StopAccessState:
    stop_state = StopAccessState(from_stop_id, feed)
    stop_times = feed.stop_times

    already_processed_xfers = []
    just_updated_stops = [from_stop_id]

    for k in range(transfer_limit + 1):
        logger.debug("\nAnalyzing possibilities with {} transfers".format(k))

        stop_ids = stop_state.all_stops()
        logger.debug("\tinital qualifying stop ids count: {}".format(len(stop_ids)))

        # update time to stops calculated based on stops accessible
        tic = time.perf_counter()
        _stop_times_for_kth_trip(
            stop_state, just_updated_stops, stop_times, departure_time, k
        )
        toc = time.perf_counter()
        logger.debug("\tstop times calculated in {:0.4f} seconds".format(toc - tic))

        added_keys_count = len(stop_state.all_stops()) - len(stop_ids)
        logger.debug("\t\t{} stop ids added".format(added_keys_count))

        if added_keys_count == 0:
            logger.info(
                "No valid transfers found after iteration {} for stop pair {}->{}".format(
                    k, from_stop_id, to_stop_id
                )
            )
            break

        # reset stop_ids count
        stop_ids = stop_state.all_stops()

        # now add footpath transfers and update
        tic = time.perf_counter()
        just_updated_stops_temp = just_updated_stops
        just_updated_stops = _add_footpath_transfers(
            stop_state, transfers, already_processed_xfers, k
        )
        toc = time.perf_counter()
        logger.debug(
            "\tfootpath transfers calculated in {:0.4f} seconds".format(toc - tic)
        )

        added_keys_count = len(stop_state.all_stops()) - len(stop_ids)
        logger.debug("\t\t{} stop ids added".format(added_keys_count))

        logger.debug(
            "\talready processed count increased from {} to {}".format(
                len(already_processed_xfers),
                len(already_processed_xfers + just_updated_stops_temp),
            )
        )
        logger.debug("\tnew stops to process: {}".format(len(just_updated_stops)))
        already_processed_xfers += just_updated_stops_temp

    if not stop_state.has_stop(to_stop_id):
        logger.warning(
            "Unable to find route to destination ({}->{}) within transfer limit".format(
                from_stop_id, to_stop_id
            )
        )

    return stop_state
