import os
import time
import subprocess
import requests
import pandas as pd
import glob
from time import sleep

#PARAMS
from utils import get_latest, merge_batches

SLEEP_SINGLE = 0.1  # pause between queries so that we do not flood server
SLEEP_BATCH = 10 # sleep between batches of requests, not to floog server
SLEEP_RESTART = 60 # sleep after restarting the server (if needed)
BATCH_SIZE = 50  # store results every batch_size trips and give a break to a server
RESTART_EVERY = 999999 # restart after every batch

OTP_API = "http://localhost:8080/otp/routers/default/plan"
QUERY_MODES = "TRANSIT,WALK"
MAX_WALK_DISTANCE = 2000



def make_query(row):
    """
    creates OTP query from the single row in requests
    """
    query = dict()
    query['fromPlace'] = "{},{}".format(row.origin_y, row.origin_x)
    query['toPlace'] = "{},{}".format(row.destination_y, row.destination_x)
    hour, minute = row.treq.hour, row.treq.minute
    if int(hour) < 12:
        ampm = 'am'
    else:
        hour = int(hour) - 12
        ampm = 'pm'
    query['time'] = "{:02d}:{}{}".format(int(hour), minute, ampm)

    query['date'] = "{}-{}-{}".format(row.treq.month, row.treq.day, row.treq.year)

    query['mode'] = QUERY_MODES
    query['maxWalkDistance'] = MAX_WALK_DISTANCE
    query['arriveBy'] = 'false'
    return query


def parse_OTP_response(response):
    '''
    parses OTP response (.json) and populates dictionary of PT trip attributes
    :param response: OTP server response
    :param store_modes: do we store information on modes
    :return: one row of resulting database
    '''
    if 'plan' in response.keys():
        plan = response['plan']
        modes = list()
        shortest = 0
        duration = 99999
        # find the shortest
        for it in plan['itineraries']:
            dur = it['duration']
            if dur < duration:
                duration = dur
                shortest = it

        total_fare = 0
        # Track combinations of operator and mode that we've seen
        encountered_operator_modes = set()
        for leg in shortest['legs']:
            mode = leg['mode']
            leg_duration = int(leg['duration'])
            leg_distance = int(leg['distance'])
            
            # Get operator if available (default to None if not present)
            operator = leg.get('agencyId') or leg.get('agency') or leg.get('operator')
            
            # Combine operator and mode to create a unique key
            operator_mode_key = f"{operator}_{mode}" if operator else mode
            
            # Check if this is a new operator-mode combination and if the mode has a base fare
            if mode in mode_base_fares and operator_mode_key not in encountered_operator_modes:
                encountered_operator_modes.add(operator_mode_key)
                total_fare += mode_base_fares[mode]
            
            # Calculate distance-based fare for this leg
            distance_km = leg_distance / 1000  # Assuming distance is in meters
            price_per_km = mode_prices.get(mode, 0)  # Default to 0 if mode not found
            leg_fare = price_per_km * distance_km
            total_fare += leg_fare
            
            # Append leg information to modes list
            modes.append([leg['mode'], leg_duration, leg_distance])
    
        # Round the fare to 2 decimal places
        total_fare = round(total_fare, 2)

        ret = {'success': True,
               "n_itineraries": len(plan['itineraries']),
               'duration': shortest['duration'],
               'walkDistance': shortest['walkDistance'],
               'transfers': shortest['transfers'],
               'transitTime': shortest['transitTime'],
               'waitingTime': shortest['waitingTime'],
               'fare': total_fare,
               'modes': modes}
    else:
        ret = {'success': False}
    # ret_str = """Trip from ({:.4f},{:.4f}) to ({:.4f},{:.4f}) at {}.
    # \n{} connections found. \nBest one is {:.0f}min ({:.0f}m walk, {} transfer(s), wait time {:.2f}min)""".format(ret)
    return ret


def query_dataset(PATH, OUTPATH, BATCHES_PATH = None):
    df = pd.read_csv(PATH, index_col=[0]).sort_index()  # load the csv
    df.treq = pd.to_datetime(df.treq)
    first_index = get_latest(OUTPATH)
    if first_index > 0:
        df = df.loc[first_index:]
    print('trips processed so far: ', first_index)
    print('trips to process ', df.shape[0])

    # loop over batches
    for batch in range(((max(BATCH_SIZE, df.shape[0]) - 1) // BATCH_SIZE + 1)):
        end_batch_idx = BATCH_SIZE * (batch + 1) if (BATCH_SIZE * (batch + 1)) <= df.shape[0] else df.shape[0]
        batch_df = df.iloc[BATCH_SIZE * batch:end_batch_idx]  # process this batch only
        print(BATCH_SIZE * batch,end_batch_idx)
        queries = batch_df.apply(make_query, axis=1)  # make OTP query for each trip in dataset

        ret_list = list()
        for id, query in queries.items():
            try:
                r = requests.get(OTP_API, params=query)
                ret = parse_OTP_response(r.json())
            except Exception as err:
                print(f"Exception occured: {err}")
                ret = {'success': False}
                pass
            ret['id'] = id
            print(id, ret['success'])
            if not ret['success']:
                print('Not found for: ', query)
            ret_list.append(ret)
            sleep(SLEEP_SINGLE)  # Time in seconds

        if len(ret_list) > 0:
            batch_out = pd.DataFrame(ret_list).set_index('id').sort_index()

            batch_name = '{}_{}.csv'.format(batch_df.index.min(), batch_df.index.max())
            batch_out.to_csv(os.path.join(BATCHES_PATH, batch_name))

            print('batch {} saved with {} out of {} trips success'.format(batch,
                                                                          batch_out[batch_out.success].shape[0],
                                                                          BATCH_SIZE))
            sleep(SLEEP_BATCH)  # Time in seconds
        if batch >= RESTART_EVERY:
            print("scheduled server restart")
            return -1
    return 1


def main(start_server = True):
    OTP_PATH = "otp-2.3.0-shaded.jar" # path to OTP executable
    CITY_PATH = "data"  # folder with osm, gtfs files and/or graph.obj

    PATH = 'georequests.csv'  # path with trips to query
    OUTPATH = PATH[:-4] + "_PT.csv"

    BATCHES_PATH = 'batches'  # path with trips to query
    if not os.path.exists(BATCHES_PATH):
        os.makedirs(BATCHES_PATH)
    # else:
        # files = glob.glob(os.path.join(BATCHES_PATH,'*'))
        # for f in files:
            # os.remove(f)
    # remove all prevoius data from batches directory


    print('starting server')
    #run java server
    if start_server:
        with open("stdout.txt", "wb") as out, open("stderr.txt", "wb") as err:
            p = subprocess.Popen(['java', '-Xmx12G', '-jar', OTP_PATH, '--build', CITY_PATH, '--inMemory'],
                                 stdout=out, stderr=err)

        while True:
            if 'Grizzly server running' in open('stdout.txt').read():
                print('server_running')
                break
            time.sleep(1)

    flag = query_dataset(PATH, OUTPATH, BATCHES_PATH)
    if flag < 0:
        print('terminating server')
        time.sleep(SLEEP_RESTART)
    else:
        print('flag positive')
    if start_server:
        p.terminate()
    print('merging processed batches')
    merge_batches(path=BATCHES_PATH, out_path=OUTPATH, remove=False)



def test_server(dataset_path):
    batch_df = pd.read_csv(dataset_path, index_col=[0]).sample(5)  # load the csv
    queries = batch_df.apply(make_query, axis=1)  # make OTP query for each trip in dataset
    print('test server on 5 sample trips')

    ret_dict = list()
    for id, query in queries.items():
        try:
            r = requests.get(OTP_API, params=query)
            ret = parse_OTP_response(r.json())
            print(query, ret)
        except Exception as err:
            print(f"Exception occured: {err}")
            ret = {'success': False}
            pass
        ret['id'] = id
        print(id, ret['success'])
        ret_dict.append(ret)


if __name__ == "__main__":

    # Define prices per kilometer for different modes
    mode_prices = {
        'BUS': 0.20, 
        'SUBWAY': 0.20, 
        'RAIL': 0.12, 
        'WALK': 0.00, 
        'BICYCLE': 0.00,  
        'TRAM': 0.20,
        'FERRY': 0.15, 
    }

    # Define base fares for different modes
    mode_base_fares = {
        'BUS': 1.0,       # €1.00 base fare
        'SUBWAY': 1.0,    # €1.00 base fare
        'RAIL': 3.0,      # €3.00 base fare
        'TRAM': 1.0,      # €1.00 base fare
        'FERRY': 1.5      # €1.50 base fare
        # Walking and cycling typically don't have base fares
    }

    main(start_server=False)