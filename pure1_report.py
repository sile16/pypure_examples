#! /usr/bin/env python3

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
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def generate_fleet_report(pure1_api_id, pure1_pk_file, pure1_pk_pwd):

    pure1Client = pure1.Client(private_key_file=pure1_pk_file,
                               private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    # Get all  Arrays, FlashArray & FlashBlade.
    response = pure1Client.get_arrays()

    arrays = []
    if response is not None:
        arrays = list(response.items)

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
            total = len(arrays)
            for array in arrays:
                if 'FA' in array.os:
                    # FlashArray Metrics
                    metrics_names = ['array_total_capacity', 'array_data_reduction', 'array_shared_space', 
                                    'array_volume_space', 'array_snapshot_space', 'array_system_space', 
                                    'array_total_load']
                else:
                    # FlashBlade Metrics
                    metrics_names = ['array_total_capacity', 'array_data_reduction', 
                                    'array_file_system_space', 'array_object_store_space' ]
                
                # We are required to specify a start and end date
                # so just grabbing a couple days and will use the last day value returned.
                start = int((datetime.datetime.now() - datetime.timedelta(days=REPORTING_INTERVAL_DAYS)).timestamp())
                end = int((datetime.datetime.now()).timestamp())

                # Request the last 7 days of data, each day metric summarized by the max value.
                # for this particular array.
                response = pure1Client.get_metrics_history(aggregation='max', names=metrics_names, resource_ids=array.id,
                                                        resolution=METRIC_RESOLUTION_DAY, start_time=start, end_time=end)
                
                # Check to see if we should delay so we don't hit an API limit
                # add some delays in here  if we get too close to limit.
                progress(count, len(arrays), 'Getting array metrics.                           ')
                if int(response.headers.x_ratelimit_remaining_second) == 1:
                    #progress(count, len(arrays), "Sleeping because of API per 5 sec rate limit")
                    time.sleep(0.5)
                if int(response.headers.x_ratelimit_remaining_second) == 0:
                    progress(count, len(arrays), "Sleeping because of API per 5 sec rate limit")
                    time.sleep(2)
                if int(response.headers.x_ratelimit_remaining_minute) < 15:
                    progress(count, len(arrays), "Sleeping because of API per minute rate limit")
                    time.sleep(2)
                if int(response.headers.x_ratelimit_remaining_minute) < 5:
                    progress(count, len(arrays), "Sleeping because of API per minute rate limit")
                    time.sleep(5)
                    
                # Increase the count for the progress bar
                count += 1
                
                # we want to null all values, so we don't accidenlty return
                # a value from the last object into the current record
                total_capacity = data_reduction = volume_space = pcnt_used = fs_space = object_space = None
                shared_space = snapshot_space = system_space = array_load = effective_space = None

                


                if hasattr(response, 'items'):
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
                                elif metric_name == 'array_total_load' : 
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
                        filewriter_fa.writerow([array.name, array.id, array.model, array.version, total_capacity, data_reduction,
                                            pcnt_used, shared_space, volume_space, snapshot_space, system_space, array_load])
                    
                    elif 'FB' in array.os:
                        try:
                            pcnt_used = round(100 * (fs_space + object_space) / total_capacity, 2)
                        except TypeError as e:
                            pass
                        #write entry into CSV file
                        filewriter_fb.writerow([array.name, array.id, array.model, array.version, total_capacity, data_reduction,
                                            pcnt_used, fs_space, object_space])
                else:
                    if response.status_code == 429 or response.status_code == 404:
                        print(response.errors[0].message)
                        if response.errors[0].context is not None:
                            print(response.errors[0].context)
                        if response.status_code == 429:
                            print("Remaining requests: " + response.headers.x_ratelimit_limit_minute) 
                    else:     
                        print(str.format("error code: {}\n error: {}", response.status_code, response.errors[0].message))
                        print(str.format(" metrics: {}", str(metrics_names)))
        



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
    progress(1,1, 'Finished                                   ')
    print("")
    print('Finished')


