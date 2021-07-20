import os
import logging

import partridge as ptg

from gtfs_router.raptor import raptor_assignment
from gtfs_router import log_stop_information

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.info('Initialize Logger')

gtfs_path = os.path.join('..', 'data', 'sacramento_2021_03_15')
_date, service_ids = ptg.read_busiest_date(gtfs_path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_geo_feed(gtfs_path, view)
logger.info('Date Selected: {}'.format(_date))

departure_time = 8.5 * 60 * 60

max_transfers = 2

#from_stop_id = '1792' #STOCKTON BLVD & ALHAMBRA BLVD (EB)
#to_stop_id = '519' #L ST & 21ST ST (WB)

from_stop_id = '1242' #MARCONI AVE & GREENWOOD AVE (WB)
to_stop_id = '7065' #Sunrise Station (EB)

log_stop_information(feed, from_stop_id)
log_stop_information(feed, to_stop_id)

stop_state = raptor_assignment(feed.stop_times, from_stop_id, to_stop_id, departure_time, feed.transfers, max_transfers)

logger.info(stop_state.describe_path(to_stop_id, feed.stops, feed.stop_times, feed.trips, feed.routes))