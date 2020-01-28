#! /usr/bin/env python3

import argparse
import datetime
import csv

from pypureclient import pure1

METRIC_RESOLUTION_DAY = 86400000
REPORTING_INTERVAL_DAYS = 7
BYTES_IN_A_TERABYTE = 1099511627776
BYTES_IN_A_GIGABYTE = 1073741824
queries_count = 1
sorted_metrics = None


def generate_fleet_report(pure1_api_id, pure1_pk_file, pure1_pk_pwd):

    pure1Client = pure1.Client(private_key_file=pure1_pk_file,
                               private_key_password=pure1_pk_pwd, app_id=pure1_api_id)

    # Get all Flash Arrays.
    # response = pure1Client.get_arrays(filter="contains(model,'FB')")
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
            for array in arrays:
                os_version = str.format("{} {}", array.os, array.version)
                if 'FA' in array.os:
                    metrics_names = ['array_total_capacity', 'array_data_reduction', 'array_shared_space', 
                                    'array_volume_space', 'array_snapshot_space', 'array_system_space', 'array_total_load']
                    if 'CBS' in array.model:
                        # Only our Capacity on demand & CBS have effective_used_space
                        metrics_names.append('array_effective_used_space')
                else:
                    metrics_names = ['array_total_capacity', 'array_data_reduction', 
                                    'array_file_system_space', 'array_object_store_space' ]
                start = int((datetime.datetime.now(
                ) - datetime.timedelta(days=REPORTING_INTERVAL_DAYS)).timestamp())
                end = int((datetime.datetime.now()).timestamp())

                # Request the last 7 days of data, each day metric summarized by the max value.
                response = pure1Client.get_metrics_history(aggregation='max', names=metrics_names, resource_ids=array.id,
                                                        resolution=METRIC_RESOLUTION_DAY, start_time=start, end_time=end)

                total_capacity = data_reduction = volume_space = pcnt_used = fs_space = object_space = None
                shared_space = snapshot_space = system_space = array_load = effective_space = None

                if hasattr(response, 'items'):
                    metrics_items = list(response.items)
                    for metric_item in metrics_items:
                        if metric_item.data:
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
                                elif metric_name == 'array_effective_used_space':
                                    effective_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                                elif metric_name == 'array_file_system_space':
                                    fs_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                                elif metric_name == 'array_object_store_space':
                                    object_space = round(metric_data[1] / BYTES_IN_A_TERABYTE, 2)
                                elif metric_name == 'array_system_space':
                                    system_space = round(metric_data[1] / BYTES_IN_A_GIGABYTE, 2)
                                elif metric_name == 'array_total_load' : 
                                    array_load = round(metric_data[1], 2)
                    
                    if 'FA' in array.os:
                        if effective_space:
                            volume_capacity = effective_space
                        else:
                            try:
                                pcnt_used = round(100 * (volume_space + shared_space +
                                        snapshot_space + system_space) / total_capacity, 2)
                            except:
                                pass
                        filewriter_fa.writerow([array.name, array.id, array.model, os_version, total_capacity, data_reduction,
                                            pcnt_used, shared_space, volume_space, snapshot_space, system_space, array_load])
                    
                    elif 'FB' in array.os:
                        try:
                            pcnt_used = round(100 * (fs_space + object_space) / total_capacity, 2)
                        except:
                            pass
                        filewriter_fb.writerow([array.name, array.id, array.model, os_version, total_capacity, data_reduction,
                                            pcnt_used, fs_space, object_space])



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
