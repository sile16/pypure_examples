#! /usr/bin/env python3

import argparse
import datetime
import csv
import sys
import time

from pypureclient import pure1

METRIC_RESOLUTION_DAY = 86400000
REPORTING_INTERVAL_DAYS = 7
BYTES_IN_A_TERABYTE = 1099511627776
BYTES_IN_A_GIGABYTE = 1073741824
queries_count = 1
sorted_metrics = None

# progress bar.
def progress(count, total, status=''):
# The MIT License (MIT)
# Copyright (c) 2016 Vladimir Ignatev
#
# Permission is hereby granted, free of charge, to any person obtaining 
# a copy of this software and associated documentation files (the "Software"), 
# to deal in the Software without restriction, including without limitation 
# the rights to use, copy, modify, merge, publish, distribute, sublicense, 
# and/or sell copies of the Software, and to permit persons to whom the Software 
# is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included 
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
# OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
# OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def get_metrics(pure1Client, array, arrays, count):
    # Return array metrics, or None if failed.
    # automatically sleep and retry if API rate limit hit.

    if 'FA' in array.os or 'Purity' in array.os:
        # FlashArray Metrics
        metrics_names = ['array_total_capacity', 'array_data_reduction', 'array_shared_space', 
                        'array_volume_space', 'array_snapshot_space', 'array_system_space', 
                        'array_total_load']
    elif 'FB' in array.os:
        # FlashBlade Metrics
        metrics_names = ['array_total_capacity', 'array_data_reduction', 
                        'array_file_system_space', 'array_object_store_space' ]
    else:
        # Error unknown os, what is this? 
        print(f"Unknown Pure OS Type: {array.os}")
        return None
    
    # We are required to specify a start and end date
    # so just grabbing a couple days and will use the last day value returned.
    start = int((datetime.datetime.now() - datetime.timedelta(days=REPORTING_INTERVAL_DAYS)).timestamp())
    end = int((datetime.datetime.now()).timestamp())

    # Request the last 7 days of data, each day metric summarized by the max value.
    # for this particular array.
    response = pure1Client.get_metrics_history(aggregation='max', names=metrics_names, resource_ids=array.id,
                                            resolution=METRIC_RESOLUTION_DAY, start_time=start, end_time=end)
    
    max_sleep = 30 # maximum of 30 seconds to sleep before failing
    curr_sleep_total = 0
    while response.status_code == 429:
        # This means a rate limit error, we will retry until get good results.
        # check to see if we've already waited log enough and abor
        if curr_sleep_total > max_sleep:
            # Already been waiting we need to abort
            print(f"Max sleep time timeout of {max_sleep} exceeded. Aborting")
            display_response_error(response)
            return None
        
        sleep_time = 1 # default sleep is 1 second
        if int(get_metrics.last_response.headers.x_ratelimit_remaining_minute) < 2:
            # sleep to next minute, find  remaining seconds in current
            # minute and sleep to the end of it.  But not more than 
            # the maximum sleep time.
            # this code relies on both the local server and remote having
            # accurate time. So may not be the best method of rate limiting.

            current_seconds = time.localtime().tm_sec
            if current_seconds > 20:
                # picking greater than 2, because what if clock are off a bit
                # don't want to wait 30 seconds for nor eason.
                sleep_time = min(60 - current_seconds, 30) 
            else:
                # we hit the per minute maximum, so wait a little bit longer.
                sleep_time = 5
        
        progress(count, len(arrays), f'API Rate limit hit, sleeping for {sleep_time} second(s).')
        time.sleep(sleep_time)  # actually do the sleep.
        curr_sleep_total += sleep_time #keep track of total time spent sleeping

        # retry the API call after sleeping.
        response = pure1Client.get_metrics_history(aggregation='max', names=metrics_names, resource_ids=array.id,
                                            resolution=METRIC_RESOLUTION_DAY, start_time=start, end_time=end)
    
    if response.status_code != 200:
        display_response_error(response)
        return None

    #save the last response
    get_metrics.last_response = response

    return response

# create static variable to keep track of last repsone
# in order to save the rate limit values.
get_metrics.last_respone = None


def display_response_error(response):
    # means not 429 and not successful so, something bad
    # Do you have internet access ?
    print("") # because of progress bar, go to next line
    print(" Error getting results. Check internet access.")
    print(response.errors[0].message)
    if response.errors[0].context is not None:
        print(response.errors[0].context)


def generate_fleet_report(pure1_api_id, pure1_pk_file, pure1_pk_pwd):

    pure1Client = pure1.Client(private_key_file=pure1_pk_file,
                               private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    # Get all  Arrays, FlashArray & FlashBlade.
    response = pure1Client.get_arrays()

    # Check to make sure we successfully connected, 200=OK
    if response.status_code != 200:
        display_response_error(response)
        return
    
    # this gets all the response items which is a
    # generator which has no length, by pulling all into a
    # list it has a length.
    arrays = list(response.items)
    if len(arrays) == 0:
        print("Error: No arrays returned by Pure1 API ")
        return

    with open('pure1_report_fa.csv', 'w') as csvfile_fa:
        with open('pure1_report_fb.csv', 'w') as csvfile_fb:
            # Create two CSV files for writing to
            filewriter_fa = csv.writer(csvfile_fa, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter_fa.writerow(['Array Name', 'Array ID', 'Model', 'OS Version',
                                'Total Capacity (TB)', 'Data Reduction', '% Used',
                                'Shared Space (TB)', 'Volume Space (TB)',
                                'Snapshot Space (TB)', 'System Space (GB)', 'Max Load 24Hours'])
            
            filewriter_fb = csv.writer(csvfile_fb, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter_fb.writerow(['Array Name', 'Array ID', 'Model', 'OS Version',
                                'Total Capacity (TB)', 'Data Reduction', '% Used',
                                'File System Space (TB)', 'Object Store Space (TB)'])

            # Go through all arrays
            count = 0
            for array in arrays:
                progress(count, len(arrays), "Getting array metrics...                      ")
                response = get_metrics(pure1Client, array, arrays, count)
                if not response:
                    # already printed error message  if response is None.
                    return
                    
                # Increase the count for the progress bar
                count += 1
                
                # we want to null all values, so we don't accidenlty return
                # a value from the last object into the current record
                total_capacity = data_reduction = volume_space = pcnt_used = fs_space = object_space = None
                shared_space = snapshot_space = system_space = array_load = effective_space = None

                # I think items is a generator and will 
                # return all the items as you request lazily, doing a list()
                # forces it to pull in all items into an easy python construct
                metrics_items = list(response.items)

                # we requested several metrics
                for metric_item in metrics_items:
                    if metric_item.data:
                        # Each metric has multiple data points, 1 each day for 7 days.
                        # they are  from older to newer, so we will keep the last value
                        # we find.
                        for metric_data in metric_item.data:
                            metric_name = metric_item.name
                            if metric_name == 'array_total_capacity':
                                total_capacity = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_data_reduction':
                                data_reduction = round(metric_data[1], 2)
                            elif metric_name == 'array_volume_space':
                                volume_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_shared_space':
                                shared_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_snapshot_space':
                                snapshot_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_file_system_space':
                                fs_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_object_store_space':
                                object_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                            elif metric_name == 'array_system_space':
                                system_space = round(metric_data[1] / BYTES_IN_A_GIGABYTE, 2)
                            elif metric_name == 'array_total_load': 
                                array_load = round(metric_data[1], 2)
                
                if 'FA' in array.os:
                    try:
                        # If for some reason it didn't return data, the value will
                        # be null and this will be a TypeError
                        # but we still want to capture the record even if there is a 
                        # bad value.
                        pcnt_used = round(100 * (volume_space + shared_space +
                                snapshot_space + system_space) / total_capacity, 2)
                    except TypeError as e:
                        pass
                    # Write entry into CSV file.
                    filewriter_fa.writerow([array.name, array.id, array.model, array.version, 
                                            total_capacity, data_reduction,
                                            pcnt_used, shared_space, volume_space, 
                                            snapshot_space, system_space, array_load])
                
                elif 'FB' in array.os:
                    try:
                        # Even if a value isn't returned and this fails we keep going.
                        pcnt_used = round(100 * (fs_space + object_space) / total_capacity, 2)
                    except TypeError as e:
                        pass
                    #write entry into CSV file
                    filewriter_fb.writerow([array.name, array.id, array.model, array.version, 
                                            total_capacity, data_reduction,
                                            pcnt_used, fs_space, object_space])

            progress(1,1, 'Finished, {} result(s) saved into 2 csv files.'.format(len(arrays)))



if __name__ == '__main__':

    _ = argparse.ArgumentParser(
        description='Pure1 Reporting integrtion parameters')
    _.add_argument('pure1_api_id', type=str, help='Pure1 API Client App ID.')
    _.add_argument('pure1_pk_file', type=str,
                   help='Pure1 API Client Private Key File')
    _.add_argument('-p', '--password', type=str,
                   help="use if private key is encrypted (or use keyboard prompt)")

    ARGS = _.parse_args()
    print("Generating Pure1 custom report")
    generate_fleet_report(ARGS.pure1_api_id, ARGS.pure1_pk_file, ARGS.password)
    # Make progress bar hit 100%  because.... that's nice.
    
    print("")


