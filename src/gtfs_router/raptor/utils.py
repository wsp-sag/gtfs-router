import logging
import time

import pandas as pd

from stop_access import StopAccessState
from typing import List

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.debug('utils')


def get_trip_ids_for_stop_new(stop_times: pd.DataFrame, stop_ids: List[str], departure_time: int):
    """Takes a stop and departure time and get associated trip ids."""
    mask_1 = stop_times.stop_id.isin(stop_ids)
    mask_2 = stop_times.departure_time >= departure_time
    #mask_3 = stop_times.departure_time <= departure_time + 120 * 60

    # extract the list of qualifying trip ids
    potential_trips = stop_times[mask_1 & mask_2][['trip_id', 'stop_id', 'arrival_time', 'stop_sequence']].drop_duplicates()
    return potential_trips


def remove_prior_trips(potential_trips: pd.DataFrame, prior_trips: List[tuple]):
    tuples_in_df = pd.MultiIndex.from_frame(potential_trips[['stop_id', 'trip_id']])
    return potential_trips[~tuples_in_df.isin(prior_trips)].copy()


def stop_times_for_kth_trip(
        stops_state: StopAccessState,
        last_updated_stops: List[str],
        stop_times: pd.DataFrame,
        departure_time: float,
        #trips: pd.DataFrame,
        k
) -> None:
    tic = time.perf_counter()
    # find all trips already related to these stop
    prior_trips = stops_state.get_preceding_trips(last_updated_stops)

    # find all qualifying potential trips with these stops
    potential_trips = get_trip_ids_for_stop_new(stop_times, last_updated_stops, departure_time)

    if prior_trips:
        potential_trips = remove_prior_trips(potential_trips, prior_trips)

    # trip_stop_pairings = potential_trips.groupby('trip_id')['stop_id'].apply(list).to_dict()
    toc = time.perf_counter()
    logger.debug("\t\tTrip Pairings calculated in {:0.4f} seconds".format(toc - tic))

    tic = time.perf_counter()

    last_stop_evaluated = potential_trips.loc[potential_trips.groupby('trip_id')['arrival_time'].idxmax()]
    last_stop_states = stops_state.get_stops(list(last_stop_evaluated['stop_id'].unique()))
    last_stop_states = pd.DataFrame(index=last_stop_states.keys(), data=last_stop_states.values())
    last_stop_evaluated = pd.merge(last_stop_evaluated, last_stop_states, left_on='stop_id', right_index=True, how='left')

    last_stop_evaluated = pd.merge(stop_times, last_stop_evaluated, on='trip_id', suffixes=['', '_preceding'])
    last_stop_evaluated = last_stop_evaluated[last_stop_evaluated['stop_sequence'] >= last_stop_evaluated['stop_sequence_preceding']].copy()
    last_stop_evaluated['arrive_time_adjusted'] = last_stop_evaluated['arrival_time'] - departure_time + last_stop_evaluated['time_to_reach']

    #routes_evaluated = trips[trips['trip_id'].isin(last_stop_evaluated['trip_id'].unique())]
    #logger.debug('Routes Evaluated: {}'.format(routes_evaluated['route_id'].unique()))

    for arrive_stop_id, arrive_time_adjusted, trip_id, preceding_stop, preceding_path in \
        last_stop_evaluated[['stop_id', 'arrive_time_adjusted', 'trip_id', 'stop_id_preceding', 'preceding']].itertuples(index=False, name=None):

        stops_state.try_add_update(
            arrive_stop_id,
            arrive_time_adjusted,
            trip_id,
            preceding_stop,
            preceding_path,
            k*2
        )

    toc = time.perf_counter()
    logger.debug("\t\t'Iterate' New Trip Pairings calculated in {:0.4f} seconds".format(toc - tic))


def add_footpath_transfers(
        stops_state: StopAccessState,
        transfers: pd.DataFrame,
        already_processed_stops: List[str],
        k,
) -> List[str]:
    # initialize a return object
    updated_stop_ids = []

    # add in transfers to nearby stops
    stop_ids = stops_state.all_stops()

    stop_xfers = transfers[(transfers['from_stop_id'].isin(stop_ids)) &
                           (~transfers['from_stop_id'].isin(already_processed_stops))].copy()

    ref_stop_state = [{'from_stop_id': stop_id, 'time_to_reach': vals['time_to_reach'], 'preceding': vals['preceding']}
                      for stop_id, vals in stops_state._stops.items()]

    ref_stop_state = pd.DataFrame(ref_stop_state)

    stop_xfers = pd.merge(stop_xfers, ref_stop_state, on='from_stop_id')

    stop_xfers['arrive_time_adjusted'] = stop_xfers['time_to_reach'] + stop_xfers['min_transfer_time']
    stop_xfers = stop_xfers.loc[stop_xfers.groupby('to_stop_id')['arrive_time_adjusted'].idxmin()]
    stop_xfers['last_trip_id'] = stop_xfers.apply(lambda x: x['preceding'][-1] if len(x['preceding']) else '', axis=1)

    for i, row in stop_xfers.iterrows():
        did_update = stops_state.try_add_update(
            row['to_stop_id'],
            row['arrive_time_adjusted'],
            row['last_trip_id'],
            preceding_stop=row['from_stop_id'],
            k=k + 1
        )

        if did_update:
            updated_stop_ids.append(row['to_stop_id'])

    return updated_stop_ids