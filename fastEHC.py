import argparse
import os
import re
import ijson
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from collections import defaultdict
import math
import csv

try:
    from tqdm import tqdm
    tqdm_available = True
except ImportError:
    tqdm_available = False
    print("Consider installing tqdm for progress bar: 'pip install tqdm'")

# for direct integration with Excel workbook
try:
    import pandas as pd
    from openpyxl import load_workbook
    from openpyxl.utils.exceptions import InvalidFileException
    from openpyxl.utils import column_index_from_string
    xl_available = True
except ImportError:
    xl_available = False   

## for debugging only
import pprint

# Global variable(s)
cc_snapshot_seconds = 1 # the size of concurrency snapshots in seconds
excel_sheet = "Data" # the name of the Excel sheet where the data goes



def ingest_file(file_path):
    print("Reading data file...", end="", flush=True)
    scans = []
    tmp_field_names = []
    with open(file_path, 'rb') as file:
        # Extract field names from the @odata.context string
        context = ijson.items(file, '@odata.context')
        context_str = next(context)
        pattern = r"#Scans\((.*?)\)"
        match = re.search(pattern, context_str)
        if match:
            fields_str = match.group(1)
            tmp_field_names = [field.strip() for field in fields_str.split(',')]
            # Adjust the field names here, using tmp_field_names
            field_names = [field.replace('(LanguageName', '') if 'ScannedLanguages' in field else field for field in tmp_field_names]

        # Reset file pointer and extract scan items
        file.seek(0)
        for scan in ijson.items(file, 'value.item'):
            scans.append(scan)

    print("completed!")
    return field_names, scans



def calculate_time_difference(t1, t2):
    dt1 = parse_date(t1)
    dt2 = parse_date(t2)
    time_diff = (dt2 - dt1).total_seconds()
    
    return time_diff


# Output a data structure to the Excel 'Data' sheet starting at the indicated cell (e.g., J4)
def write_to_excel(data, start_col, start_row):
    try:
        # Convert start_col from letters to a numerical index
        start_col_index = column_index_from_string(start_col)
        wb_sheet = workbook[excel_sheet]
        
        for row_offset, row_data in enumerate(data, start=0):
            for col_offset, value in enumerate(row_data, start=0):
                # Calculate actual row and column indices
                row_idx = start_row + row_offset
                col_idx = start_col_index + col_offset
                wb_sheet.cell(row=row_idx, column=col_idx, value=value)
    except IOError as e:
        print(f"IOError when writing to Excel file: {e}")
        return
    except Exception as e:
        print(f"Unexpected error when writing to the Excel file: {e}")
        return


# Output a data structure to a csv file
def write_to_csv(header, data, filename):
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(data)
    except IOError as e:
        print(f"IOError when writing to file: {e}")
    except Exception as e:
        print(f"Unexpected error when creating/writing to the CSV file: {e}")



# Process the scan data.
# One single function will be more efficient but start to get messy. Brace youreself.
def process_scans(scans, full_csv):

    # define (most) variables and data structures

    # date range of data
    first_date = datetime.max.date()
    last_date = datetime.min.date()

    # general scan stats
    sum_scan_count = yes_scan_count = no_scan_count = 0
    scan_stats_by_date = {}
    scanned_projects = {}

    # results info
    results = {
        "total_vulns__sum": 0, "high__sum": 0, "medium__sum": 0, "low__sum": 0, "info__sum": 0, 
        "total_vulns__max": 0, "high__max": 0, "medium__max": 0, "low__max": 0, "info__max": 0, 
        "total_vulns__avg": 0, "high__avg": 0, "medium__avg": 0, "low__avg": 0, "info__avg": 0,
        "high_results__scan_count": 0, "medium_results__scan_count": 0, "low_results__scan_count": 0, "info_results__scan_count": 0, "zero_results__scan_count": 0}

    # presets
    preset_names = {}
    
    # languages
    scanned_languages = {}

    # scan origins
    origins = {}
    printable_origins = {
        "ADO": "ADO",
        "Bamboo": "Bamboo",
        "CLI": "CLI",
        "cx-CLI": "cx-CLI",
        "CxFlow": "CxFlow",
        "Eclipse": "Eclipse",
        "cx-intellij": "IntelliJ",
        "Jenkins": "Jenkins",
        "Manual": "Manual",
        "Maven": "Maven",
        "Other": "Other",
        "System": "Scheduled",
        "TeamCity": "TeamCity",
        "TFS": "TFS",
        "Visual Studio": "Visual Studio",
        "Visual-Studio-Code": "Visual Studio Code",
        "VSTS": "VSTS",
        "Web Portal": "Web Portal",
        "MISSING ORIGIN TYPE": "Missing Origin Type"
    }
    grouped_origins = {value: 0 for value in printable_origins.values()}

    # bins to track scan info based on LOC range (count and various time data)
    size_bins = {
        '0 to 20k': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '20k-50k': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '50k-100k': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '100k-250k': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '250k-500k': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '500k-1M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '1M-2M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '2M-3M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '3M-5M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '5M-7M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '7M-10M': {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0},
        '10M+':  {"yes_scan_count": 0, "no_scan_count": 0, "total_scan_time__sum": 0, "source_pulling_time__sum": 0, "queue_time__sum": 0,
        "engine_scan_time__sum": 0, "total_scan_time__max": 0, "source_pulling_time__max": 0, "queue_time__max": 0, "engine_scan_time__max": 0,
        "total_scan_time__avg": 0, "source_pulling_time__avg": 0, "queue_time__avg": 0, "engine_scan_time__avg": 0}
    }

    # Variables for concurrency
    # Event format: (timestamp, change_in_count, event_type)
    # Snapshot format: (timestamp, active_engines, queue_length)
    # change_in_count is +1 for starts (entering queue or starting engine) and -1 for ends (leaving queue or engine finishing)
    # event_type distinguishes between 'queue' and 'engine'
    cc_events = filtered_cc_events = snapshot_metrics = []
    
    # Prepare to output CSV of all scan data and create output file, if required
    if full_csv['enabled']:
        try:
            filename = os.path.join(full_csv['csv_dir'], f"00-full_scan_data.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(full_csv['field_names'])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Initialize tqdm object; we exclude concurrency processing because it's so fast, even for massive data sets
    if tqdm_available:
        pbar = tqdm(total=len(scans), desc="Processing scans")
    else:
        print("Processing scans...", end="", flush=True)

    # process all the things
    for scan in scans:
        if tqdm_available:
            pbar.update(1)
            pbar.refresh()
        
        # If required, we want to output to the full scan CSV first so as to include scans with missing fields (such as loc). This will cause a potential
        # mismatch between record counts but shouldn't impact anything relating to metrics or analysis. This CSV is only used for manual analysis.
        if full_csv['enabled']:
            try:
                filename = os.path.join(full_csv['csv_dir'], f"00-full_scan_data.csv")
                with open(filename, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    # Build a row by extracting each field from the scan in the order of field_names
                    row = []
                    for field in full_csv['field_names']:
                        if field == 'ScannedLanguages':
                            # Special handling for ScannedLanguages field to convert list of dicts to comma-separated string
                            languages = scan.get(field, [])
                            language_str = ', '.join(lang['LanguageName'] for lang in languages)
                            row.append(language_str)
                        else:
                            # For all other fields, use the value as-is
                            row.append(scan.get(field, ""))
                    # Write the constructed row to the CSV file
                    writer.writerow(row)
            except IOError as e:
                print(f"IOError when writing to file: {e}")
            except Exception as e:
                print(f"Unexpected error when creating/writing to the CSV file: {e}")

        # If there is no LOC value, we might as well just completely skip the scan.
        # This differs from the current process but ensures that scan counts actually match in various metrics. Also, other fields are typically also missing.
        loc = scan.get('LOC', None)
        if loc is None:
            continue

        # update the date range
        scan_date_str = scan.get('ScanRequestedOn', '').split('T')[0]
        scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d").date()
        first_date = min(first_date, scan_date)
        last_date = max(last_date, scan_date)

        # determine the correct bin key
        if loc <= 20000:
            bin_key = '0 to 20k'
        elif loc <= 50000:
            bin_key = '20k-50k'
        elif loc <= 100000:
            bin_key = '50k-100k'
        elif loc <= 250000:
            bin_key = '100k-250k'
        elif loc <= 500000:
            bin_key = '250k-500k'
        elif loc <= 1000000:
            bin_key = '500k-1M'
        elif loc <= 2000000:
            bin_key = '1M-2M'
        elif loc <= 3000000:
            bin_key = '2M-3M'
        elif loc <= 5000000:
            bin_key = '3M-5M'
        elif loc <= 7000000:
            bin_key = '5M-7M'
        elif loc <= 10000000:
            bin_key = '7M-10M'
        else:
            bin_key = '10M+'

        # general stats + bin metrics; only update engine time if there was actually a scan
        if scan_date not in scan_stats_by_date:
            scan_stats_by_date[scan_date] = {
                'total_scan_count': 0,
                'yes_scan_count': 0,
                'no_scan_count': 0,
                'full_scan_count': 0,
                'incremental_scan_count': 0,
                'loc__sum': 0,
                'loc__max': 0,
                'failed_loc__sum': 0,
                'failed_loc__max': 0
            }

        scan_stats_by_date[scan_date]['total_scan_count'] += 1
        scan_stats_by_date[scan_date]['loc__sum'] += loc
        scan_stats_by_date[scan_date]['loc__max'] = max(loc, scan_stats_by_date[scan_date]['loc__max'])
        scan_stats_by_date[scan_date]['failed_loc__sum'] += scan.get('FailedLOC', 0)
        scan_stats_by_date[scan_date]['failed_loc__max'] = max(scan.get('FailedLOC', 0), scan_stats_by_date[scan_date]['failed_loc__max'])
        
        if scan.get('IsIncremental', None):
            scan_stats_by_date[scan_date]['incremental_scan_count'] += 1
        else:
            scan_stats_by_date[scan_date]['full_scan_count'] += 1
        
        # sometimes one of these fields is empty
        project_id = scan.get('ProjectId', 0)
        project_name = scan.get('ProjectName', "")
        pid = str(project_id) + "_" + project_name

        if pid not in scanned_projects:
            scanned_projects[pid] = {
                'id': project_id,
                'project_name': project_name,
                'project_scan_count': 0,
                'total_vulns_count': 0,
                'high_count': 0,
                'medium_count': 0,
                'low_count': 0,
                'info_count': 0,
            }

        scanned_projects[pid]['project_scan_count'] += 1
        scanned_projects[pid]['total_vulns_count'] = scan.get('TotalVulnerabilities', 0)
        scanned_projects[pid]['high_count'] = scan.get('High', 0)
        scanned_projects[pid]['medium_count'] = scan.get('Medium', 0)
        scanned_projects[pid]['low_count'] = scan.get('Low', 0)
        scanned_projects[pid]['info_count'] = scan.get('Info', 0)

        source_pulling_time = math.ceil(calculate_time_difference(scan.get('ScanRequestedOn'),scan.get('QueuedOn')))
        queue_time = math.ceil(calculate_time_difference(scan.get('QueuedOn'),scan.get('EngineStartedOn')))
        total_scan_time = math.ceil(calculate_time_difference(scan.get('ScanRequestedOn'),scan.get('ScanCompletedOn')))

        bin = size_bins[bin_key]

        bin['source_pulling_time__sum'] += source_pulling_time
        bin['queue_time__sum'] += queue_time
        bin['total_scan_time__sum'] += total_scan_time
        bin['source_pulling_time__max'] = max(source_pulling_time, bin['source_pulling_time__max'])
        bin['queue_time__max'] = max(queue_time, bin['queue_time__max'])
        bin['total_scan_time__max'] = max(total_scan_time, bin['total_scan_time__max'])

        if scan.get('EngineFinishedOn', None) is not None:
            engine_scan_time = math.ceil(calculate_time_difference(scan.get('EngineStartedOn'),scan.get('EngineFinishedOn')))
            yes_scan_count += 1
            scan_stats_by_date[scan_date]['yes_scan_count'] += 1
            bin['yes_scan_count'] += 1
            bin['engine_scan_time__sum'] += engine_scan_time
            bin['engine_scan_time__max'] = max(engine_scan_time, bin['engine_scan_time__max'])
        else:
            no_scan_count +=1
            scan_stats_by_date[scan_date]['no_scan_count'] += 1
            bin['no_scan_count'] += 1

        # results info
        results['total_vulns__sum'] += scan.get('TotalVulnerabilities', 0)
        results['high__sum'] += scan.get('High', 0)
        results['medium__sum'] += scan.get('Medium', 0)
        results['low__sum'] += scan.get('Low', 0)
        results['info__sum'] += scan.get('Info', 0)
        results['total_vulns__max'] = max(results['total_vulns__max'], scan.get('TotalVulnerabilities', 0))
        results['high__max'] = max(results['high__max'], scan.get('High', 0))
        results['medium__max'] = max(results['medium__max'], scan.get('Medium', 0))
        results['low__max'] = max(results['low__max'], scan.get('Low', 0))
        results['info__max'] = max(results['info__max'], scan.get('Info', 0))
        if scan.get('High', 0) > 0:
            results['high_results__scan_count'] += 1
        if scan.get('Medium', 0) > 0:
            results['medium_results__scan_count'] += 1
        if scan.get('Low', 0) > 0:
            results['low_results__scan_count'] += 1
        if scan.get('Info', 0) > 0:
            results['info_results__scan_count'] += 1
        if scan.get('TotalVulnerabilities', 0) == 0:
            results['zero_results__scan_count'] += 1
        
        # presets
        preset_name = scan.get('PresetName')
        preset_names[preset_name] = preset_names.get(preset_name, 0) + 1
        
        # languages
        for language in scan.get('ScannedLanguages', []):
            lang_name = language.get('LanguageName')
            if lang_name and lang_name != "Common":
                scanned_languages[lang_name] = scanned_languages.get(lang_name, 0) + 1

        # scan origins
        origin = scan.get('Origin', 'Unknown')
        origins[origin] = origins.get(origin, 0) + 1

        # parse timestamps for concurrency queueing and engine events
        queued_on = parse_date(scan['QueuedOn']).timestamp()
        engine_started_on = parse_date(scan['EngineStartedOn']).timestamp()
        engine_finished_on = None
        optimal_scan_finish = None

        cc_events.append((queued_on, +1, 'queue'))
        cc_events.append((engine_started_on, -1, 'queue'))

        if 'EngineFinishedOn' in scan and scan['EngineFinishedOn'] is not None:
            engine_finished_on = parse_date(scan['EngineFinishedOn']).timestamp()
            engine_scan_duration = engine_finished_on - engine_started_on
            optimal_scan_finish = queued_on + engine_scan_duration  # Calculate based on no queue delay assumption
            cc_events.append((engine_started_on, +1, 'engine'))
            cc_events.append((optimal_scan_finish, -1, 'engine'))

    # calculate totals and averages
    total_scan_count = yes_scan_count + no_scan_count
    for bin_key, bin in size_bins.items():
        if (bin['yes_scan_count'] + bin['no_scan_count']) > 0:
            bin['source_pulling_time__avg'] = math.ceil(bin['source_pulling_time__sum'] / (bin['yes_scan_count'] + bin['no_scan_count']))
            bin['queue_time__avg'] = math.ceil(bin['queue_time__sum'] / (bin['yes_scan_count'] + bin['no_scan_count']))
            bin['total_scan_time__avg'] = math.ceil(bin['total_scan_time__sum'] / (bin['yes_scan_count'] + bin['no_scan_count']))
        if bin['yes_scan_count'] > 0:
            bin['engine_scan_time__avg'] = math.ceil(bin['engine_scan_time__sum'] / bin['yes_scan_count'])
            bin['total_scan_time__avg'] = math.ceil(bin['total_scan_time__sum'] / bin['yes_scan_count'])
    results['total_vulns__avg'] = math.ceil(results['total_vulns__sum'] / total_scan_count)
    results['high__avg'] = round(results['high__sum'] / total_scan_count)
    results['medium__avg'] = round(results['medium__sum'] / total_scan_count)
    results['low__avg'] = round(results['low__sum'] / total_scan_count)
    results['info__avg']= round(results['info__sum'] / total_scan_count)

    # group origins
    for origin, count in origins.items():
        # Determine the group for each origin
        group = next((printable_origins[key] for key in printable_origins if origin.startswith(key)), 'Other')
        # Update the grouped origins count
        grouped_origins[group] += count
    grouped_origins_2 = {origin: count for origin, count in grouped_origins.items() if count > 0}

    # Process concurrency events

    # Initialize variables
    cc_window_start_ts = datetime.combine(first_date, datetime.min.time()).timestamp()
    cc_window_end_ts = datetime.combine(last_date, datetime.min.time()).timestamp()
    num_snapshots = math.ceil((cc_window_end_ts - cc_window_start_ts) / cc_snapshot_seconds)

    # Filter out objects based on the window and sort them
    filtered_cc_events = [event for event in cc_events if cc_window_start_ts <= event[0] <= cc_window_end_ts]
    filtered_cc_events.sort(key=lambda x: x[0])

    current_active_engines = 0
    current_queue_length = 0
    event_index = 0
    snapshot_metrics = []
    
    # For each snapshot...
    for snapshot in range(num_snapshots):
        # Calculate the bounds of the snapshot in timestamp format
        snapshot_start_ts = cc_window_start_ts + snapshot * cc_snapshot_seconds
        next_snapshot_start_ts = snapshot_start_ts + cc_snapshot_seconds
        
        while event_index < len(filtered_cc_events) and filtered_cc_events[event_index][0] < next_snapshot_start_ts:
            event_time, change, event_type = filtered_cc_events[event_index]
            
            if event_type == 'engine':
                current_active_engines += change
            elif event_type == 'queue':
                current_queue_length += change
            
            event_index += 1
        
        # Convert snapshot_start_ts to datetime for recording
        snapshot_start_dt = datetime.fromtimestamp(snapshot_start_ts)

        # Append the metrics for the current snapshot to the list
        snapshot_metrics.append((snapshot_start_dt, current_active_engines, current_queue_length))

    if tqdm_available:
        pbar.close()
    else:
            print("completed!")

    return {
        'first_date': first_date,
        'last_date': last_date,
        'scan_stats_by_date': scan_stats_by_date,
        'scanned_projects': scanned_projects,
        'size_bins': size_bins,
        'results': results,
        'preset_names': preset_names,
        'scanned_languages': scanned_languages,
        'origins': grouped_origins_2,
        'cc_metrics': snapshot_metrics
    }



def format_seconds_to_hms(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


# Output to the screen as well as create very specific CSVs.
# One single function for simplification / variable reuse (but a bit messy, as well).
def output_analysis(data, csv_config, excel_config):
    total_scan_count = yes_scan_count = no_scan_count = full_scan_count = incremental_scan_count = date_max_scan_count = 0
    scan_loc__sum = scan_loc__max = scan_failed_loc__sum = scan_failed_loc__max = date_loc__max = 0
    total_scan_time__sum = source_pulling_time__sum = queue_time__sum = engine_scan_time__sum = 0
    total_scan_time__max = source_pulling_time__max = queue_time__max = engine_scan_time__max = 0
    total_scan_time__avg = source_pulling_time__avg = queue_time__avg = engine_scan_time__avg = 0
    
    day_of_week_scan_totals = {
        'Monday': 0,
        'Tuesday': 0,
        'Wednesday': 0,
        'Thursday': 0,
        'Friday': 0,
        'Saturday': 0,
        'Sunday': 0,
        'Weekday': 0,
        'Weekend': 0
    }

    daily_scan_counts = {}
    weekly_scan_counts = {}


    # unpack scan stats and crunch a few more numbers
    for scan_date, stats in data['scan_stats_by_date'].items():
        full_scan_count += stats['full_scan_count']
        incremental_scan_count += stats['incremental_scan_count']
        yes_scan_count += stats['yes_scan_count']
        no_scan_count += stats['no_scan_count']
        scan_loc__sum += stats['loc__sum']
        scan_loc__max = max(scan_loc__max, stats['loc__max'])
        scan_failed_loc__sum += stats['failed_loc__sum']
        scan_failed_loc__max = max(scan_failed_loc__max, stats['failed_loc__max'])
        daily_scan_counts[scan_date] = stats['total_scan_count']
        
        # Calculate the Monday of the current week
        monday_of_week = scan_date - timedelta(days=scan_date.weekday())
        
        # Add the count for the current week
        if monday_of_week not in weekly_scan_counts:
            weekly_scan_counts[monday_of_week] = stats['total_scan_count']
        else:
            weekly_scan_counts[monday_of_week] += stats['total_scan_count']
        
        date_loc__max = max(date_loc__max, stats['loc__sum'])
        
        if(stats['total_scan_count'] > date_max_scan_count):
            date_max_scan_count = stats['total_scan_count']
            date_max_scan_date = scan_date
       
        day_name = scan_date.strftime('%A')
        day_index = scan_date.weekday()
        day_of_week_scan_totals[day_name] += stats['total_scan_count']

        if day_index >= 5:  # Saturday or Sunday
            day_of_week_scan_totals['Weekend'] += stats['total_scan_count']
        else:
            day_of_week_scan_totals['Weekday'] += stats['total_scan_count']

    total_scan_count = yes_scan_count + no_scan_count
    high_results__scan_count = data['results']['high_results__scan_count']
    medium_results__scan_count = data['results']['medium_results__scan_count']
    low_results__scan_count = data['results']['low_results__scan_count']
    info_results__scan_count = data['results']['info_results__scan_count']
    zero_results__scan_count = data['results']['zero_results__scan_count']
    total_days = (data['last_date'] - data['first_date']).days
    total_weeks = math.ceil(total_days / 7)
    total_scan_days = len(data['scan_stats_by_date'])
            
    # Iterate through the size_bins dictionary to calculate avg and max durations (overall)
    for bin_key, bin_values in data['size_bins'].items():
        total_scan_time__sum += bin_values['total_scan_time__sum']
        total_scan_time__max = max(total_scan_time__max, bin_values['total_scan_time__max'])
        source_pulling_time__sum += bin_values['source_pulling_time__sum']
        source_pulling_time__max = max(source_pulling_time__max, bin_values['source_pulling_time__max'])
        queue_time__sum += bin_values['queue_time__sum']
        queue_time__max = max(queue_time__max, bin_values['queue_time__max'])
        engine_scan_time__sum += bin_values['engine_scan_time__sum']
        engine_scan_time__max = max(engine_scan_time__max, bin_values['engine_scan_time__max'])
    total_scan_time__avg = total_scan_time__sum / yes_scan_count
    source_pulling_time__avg = source_pulling_time__sum / yes_scan_count
    queue_time__avg = queue_time__sum / yes_scan_count
    engine_scan_time__avg = engine_scan_time__sum / yes_scan_count
    
    # Identify daily max concurrency values based on the granular calculations made previously
    daily_maxima = defaultdict(lambda: {'actual': 0, 'optimal': 0})
    overall_max_actual = 0
    overall_max_optimal = 0
    overall_max_actual_dates = set()
    overall_max_optimal_dates = set()

    for snapshot in data['cc_metrics']:
        snapshot_dt, active_engines, queue_length = snapshot
        snapshot_date = snapshot_dt.date()
        optimal_concurrency = active_engines + queue_length

        # Update daily maximums
        daily_record = daily_maxima[snapshot_date]
        daily_record['actual'] = max(daily_record['actual'], active_engines)
        daily_record['optimal'] = max(daily_record['optimal'], optimal_concurrency)

        # Update overall maximums and their dates
        if daily_record['actual'] > overall_max_actual:
            overall_max_actual = daily_record['actual']
            overall_max_actual_dates = {snapshot_date}
        elif daily_record['actual'] == overall_max_actual:
            overall_max_actual_dates.add(snapshot_date)

        if daily_record['optimal'] > overall_max_optimal:
            overall_max_optimal = daily_record['optimal']
            overall_max_optimal_dates = {snapshot_date}
        elif daily_record['optimal'] == overall_max_optimal:
            overall_max_optimal_dates.add(snapshot_date)

    # Print Summary of Scans
    print(f"\nSummary of Scans ({data['first_date']} to {data['last_date']})")
    print("-" * 50)
    print(f"Total number of scans: {format(total_scan_count, ',')}")
    print(f"- Full Scans: {format(full_scan_count, ',')} ({(full_scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Incremental Scans: {format(incremental_scan_count, ',')} ({(incremental_scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- No Code Change Scans: {format(no_scan_count, ',')} ({(no_scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Scans with High Results: {format(high_results__scan_count, ',')} ({(high_results__scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Scans with Medium Results: {format(medium_results__scan_count, ',')} ({(medium_results__scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Scans with Low Results: {format(low_results__scan_count, ',')} ({(low_results__scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Scans with Informational Results: {format(info_results__scan_count, ',')} ({(info_results__scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Scans with Zero Results: {format(zero_results__scan_count, ',')} ({(zero_results__scan_count / total_scan_count) * 100:.1f}%)")
    print(f"- Unique Projects Scanned: {format(len(data['scanned_projects']), ',')}")
    
    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"01-summary_of_scans.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Description','Value', '%'])
                writer.writerow(['Start Date',data['first_date']])
                writer.writerow(['End Date',data['last_date']])
                writer.writerow(['Days',total_days])
                writer.writerow(['Weeks',total_weeks])
                writer.writerow(['Scans Submitted',total_scan_count])
                writer.writerow(['Full Scans Submitted',full_scan_count,(full_scan_count / total_scan_count)])
                writer.writerow(['Incremental Scans Submitted',incremental_scan_count,(incremental_scan_count / total_scan_count)])
                writer.writerow(['No-Change Scans',no_scan_count,(no_scan_count / total_scan_count)])
                writer.writerow(['Scans with High Results',high_results__scan_count,(high_results__scan_count / total_scan_count)])
                writer.writerow(['Scans with Medium Results',medium_results__scan_count,(medium_results__scan_count / total_scan_count)])
                writer.writerow(['Scans with Low Results',low_results__scan_count,(low_results__scan_count / total_scan_count)])
                writer.writerow(['Scans with Informational Results',info_results__scan_count,(info_results__scan_count / total_scan_count)])
                writer.writerow(['Scans with Zero Results',zero_results__scan_count,(zero_results__scan_count / total_scan_count)])
                writer.writerow(['Unique Projects Scanned',len(data['scanned_projects'])])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")


    # Print Scan Metrics
    print("\nScan Metrics")
    print(f"- Avg LOC per Scan: {format(round(scan_loc__sum / total_scan_count), ',')}")
    print(f"- Max LOC per Scan:  {format(round(scan_loc__max), ',')}")
    print(f"- Avg Failed LOC per Scan: {format(round(scan_failed_loc__sum / yes_scan_count), ',')}")
    print(f"- Max Failed LOC per Scan:  {format(round(scan_failed_loc__max), ',')}")
    print(f"- Avg Daily LOC: {format(round(scan_loc__sum / total_scan_days), ',')}")
    print(f"- Max Daily LOC: {format(round(date_loc__max), ',')}")
    
    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"02-scan_metrics.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Description','Average', 'Max'])
                writer.writerow(['LOC per Scan',round(scan_loc__sum / total_scan_count),round(scan_loc__max)])
                writer.writerow(['Failed LOC per Scan',round(scan_failed_loc__sum / yes_scan_count),round(scan_failed_loc__max)])
                writer.writerow(['Daily LOC',round(scan_loc__sum / total_scan_days),round(date_loc__max)])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Scan Duration
    print("\nScan Duration")
    print(f"- Avg Total Scan Duration: {format_seconds_to_hms(total_scan_time__avg)}")
    print(f"- Max Total Scan Duration: {format_seconds_to_hms(total_scan_time__max)}")
    print(f"- Avg Engine Scan Duration: {format_seconds_to_hms(engine_scan_time__avg)}")
    print(f"- Max Engine Scan Duration: {format_seconds_to_hms(engine_scan_time__max)}")
    print(f"- Avg Queued Duration: {format_seconds_to_hms(queue_time__avg)}")
    print(f"- Max Queued Scan Duration: {format_seconds_to_hms(queue_time__max)}")
    print(f"- Avg Source Pulling Duration: {format_seconds_to_hms(source_pulling_time__avg)}")
    print(f"- Max Source Pulling Duration: {format_seconds_to_hms(source_pulling_time__max)}")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"03-scan_duration.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Description','Average', 'Max'])
                writer.writerow(['Total Scan Duration',format_seconds_to_hms(total_scan_time__avg),format_seconds_to_hms(total_scan_time__max)])
                writer.writerow(['Engine Scan Duration',format_seconds_to_hms(engine_scan_time__avg),format_seconds_to_hms(engine_scan_time__max)])
                writer.writerow(['Queued Duration',format_seconds_to_hms(queue_time__avg),format_seconds_to_hms(queue_time__max)])
                writer.writerow(['Source Pulling Duration',format_seconds_to_hms(source_pulling_time__avg),format_seconds_to_hms(source_pulling_time__max)])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Scan Results / Severity
    print("\nScan Results / Severity")
    print(f"- Average Total Results: {data['results']['total_vulns__avg']}")
    print(f"- Max Total Results: {data['results']['total_vulns__max']}")
    print(f"- Average High Results: {data['results']['high__avg']}")
    print(f"- Max High Results: {data['results']['high__max']}")
    print(f"- Average Medium Results: {data['results']['medium__avg']}")
    print(f"- Max Medium Results: {data['results']['medium__max']}")
    print(f"- Average Low Results: {data['results']['low__avg']}")
    print(f"- Max Low Results: {data['results']['low__max']}")
    print(f"- Average Informational Results: {data['results']['info__avg']}")
    print(f"- Max Informational Results: {data['results']['info__max']}")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"04-scan_results_severity.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Description','Average', 'Max'])
                writer.writerow(['Total',data['results']['total_vulns__avg'],data['results']['total_vulns__max']])
                writer.writerow(['High',data['results']['high__avg'],data['results']['high__max']])
                writer.writerow(['Medium',data['results']['medium__avg'],data['results']['medium__max']])
                writer.writerow(['Low',data['results']['low__avg'],data['results']['low__max']])
                writer.writerow(['Informational',data['results']['info__avg'],data['results']['info__max']])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")
    
    # Print Languages
    print("\nLanguages")
    for language_name, language_count in sorted(data['scanned_languages'].items(), key=lambda x: x[1], reverse=True):
        percentage = (language_count / total_scan_count) * 100
        print(f"- {language_name}: {format(language_count, ',')} ({percentage:.1f}%)")
    
    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"05-languages.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Language','%', 'Scans'])
                for language_name, language_count in sorted(data['scanned_languages'].items(), key=lambda x: x[1], reverse=True):
                    percentage = language_count / total_scan_count
                    writer.writerow([language_name,percentage,language_count])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Scan Submission Summary
    print("\nScan Submission Summary")
    print(f"- Average Scans Submitted per Week: {format(round(total_scan_count / total_weeks), ',')}")
    print(f"- Average Scans Submitted per Day: {format(round(total_scan_count / total_days), ',')}")
    print(f"- Average Scans Submitted per Week Day: {format(round(day_of_week_scan_totals['Weekday'] / (total_weeks * 5)), ',')}")
    print(f"- Average Scans Submitted per Weekend Day: {format(round(day_of_week_scan_totals['Weekend'] / (total_weeks * 2)), ',')}")
    print(f"- Max Daily Scans Submitted: {format(date_max_scan_count, ',')}")
    print(f"- Date of Max Scans: {date_max_scan_date}")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"06-scan_submissison_summary.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Description','Value'])
                writer.writerow(['Average Scans Submitted per Week',round(total_scan_count / total_weeks)])
                writer.writerow(['Average Scans Submitted per Day',round(total_scan_count / total_days)])
                writer.writerow(['Average Scans Submitted per Weekday',round(day_of_week_scan_totals['Weekday'] / (total_weeks * 5))])
                writer.writerow(['Average Scans Submitted per Weekend Day',round(day_of_week_scan_totals['Weekend'] / (total_weeks * 2))])
                writer.writerow(['Max Daily Scans Submitted',date_max_scan_count])
                writer.writerow(['Date of Max Scans',date_max_scan_date])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Day of Week Scan Average
    print("\nDay of Week Scan Average")
    for day_name, total_day_count in day_of_week_scan_totals.items():
        if day_name == "Weekday" or day_name == "Weekend":
            continue
        percentage = (total_day_count / total_scan_count) * 100
        print(f"- {day_name}: {format(round(total_day_count / total_weeks), ',')} ({percentage:.1f}%)")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"07-day_of_week_scan_average.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Day of Week', 'Scans', '%'])
                for day_name, total_day_count in day_of_week_scan_totals.items():
                    if day_name == "Weekday" or day_name == "Weekend":
                        continue
                    percentage = (total_day_count / total_scan_count)
                    writer.writerow([day_name,round(total_day_count / total_weeks),percentage])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Origins
    print("\nOrigins")
    for origin, origin_count in sorted(data['origins'].items(), key=lambda x: x[1], reverse=True):
        percentage = (origin_count / total_scan_count) * 100
        print(f"- {origin}: {format(origin_count, ',')} ({percentage:.1f}%)")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"08-origins.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Origin', 'Scans', '%'])
                for origin, origin_count in sorted(data['origins'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (origin_count / total_scan_count)
                    writer.writerow([origin,origin_count,percentage])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Presets
    print("\nPresets")
    for preset_name, preset_count in sorted(data['preset_names'].items(), key=lambda x: x[1], reverse=True):
        percentage = (preset_count / total_scan_count) * 100
        print(f"- {preset_name}: {format(preset_count, ',')} ({percentage:.1f}%)")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"09-presets.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Preset', 'Scans', '%'])
                for preset_name, preset_count in sorted(data['preset_names'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (preset_count / total_scan_count)
                    writer.writerow([preset_name,preset_count,percentage])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Print Scan Time Analysis
    print("\nScan Time Analysis")
    # Print the header of the table
    print(f"{'LOC Range':<12} {'Scans':<12} {'% Scans':<10} {'Avg Total':<18} {'Avg Src Pulling':<18} {'Avg Queue':<18} "
    f"{'Avg Engine':<18}")
    
    # Iterate through the size_bins dictionary to print the per-bin data
    for bin_key, bin_values in data['size_bins'].items():
        # Format times from seconds to HH:MM:SS
        source_pulling_time__avg = format_seconds_to_hms(bin_values['source_pulling_time__avg'])
        queue_time__avg = format_seconds_to_hms(bin_values['queue_time__avg'])
        engine_scan_time__avg = format_seconds_to_hms(bin_values['engine_scan_time__avg'])
        total_scan_time__avg = format_seconds_to_hms(bin_values['total_scan_time__avg'])
        
        # Print the formatted row for each bin
        print(f"{bin_key:<12} {bin_values['yes_scan_count'] + bin_values['no_scan_count']:<12,} "
        f"{(math.ceil((10000 * (bin_values['yes_scan_count'] + bin_values['no_scan_count']) / total_scan_count)) / 100):<11.2f}"
        f"{total_scan_time__avg:<18} {source_pulling_time__avg:<18} {queue_time__avg:<18} {engine_scan_time__avg:<18}")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"10-scan_time_analysis.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['LOC Range','Scans','% Scans','Avg Total Time','Avg Source Pulling Time','Avg Queue Time','Avg Engine Scan Time'])
                
                # Iterate through the size_bins dictionary to print the per-bin data
                for bin_key, bin_values in data['size_bins'].items():
                    # Format times from seconds to HH:MM:SS
                    source_pulling_time__avg = format_seconds_to_hms(bin_values['source_pulling_time__avg'])
                    queue_time__avg = format_seconds_to_hms(bin_values['queue_time__avg'])
                    engine_scan_time__avg = format_seconds_to_hms(bin_values['engine_scan_time__avg'])
                    total_scan_time__avg = format_seconds_to_hms(bin_values['total_scan_time__avg'])
                    writer.writerow([bin_key,bin_values['yes_scan_count'] + bin_values['no_scan_count'],
                        math.ceil((10000 * (bin_values['yes_scan_count'] + bin_values['no_scan_count']) / total_scan_count)) / 10000,
                        total_scan_time__avg,source_pulling_time__avg,queue_time__avg,engine_scan_time__avg])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")
    
    # Print Concurrency Summary with unique and sorted dates
    print("\nConcurrency Summary")
    print(f"- Overall Peak Actual Concurrency: {overall_max_actual} concurrent scans on {', '.join(map(str, overall_max_actual_dates))}")
    print(f"- Overall Peak Optimal Concurrency: {overall_max_optimal} concurrent scans on {', '.join(map(str, overall_max_optimal_dates))}")

    # Create output file, if required
    if csv_config['enabled']:
        try:
            filename = os.path.join(csv_config['csv_dir'], f"11-concurrency_analysis.csv")
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Date', 'Max Actual', 'Max Optimal'])
                for date, maxima in sorted(daily_maxima.items()):
                    writer.writerow([date, maxima['actual'], maxima['optimal']])
        except IOError as e:
            print(f"IOError when writing to file: {e}")
        except Exception as e:
            print(f"Unexpected error when creating/writing to the CSV file: {e}")

    # Create other output files for data that we don't print to the summary, if required
    if csv_config['enabled']:
        write_to_csv(["Date","Scans"],sorted(daily_scan_counts.items()), os.path.join(csv_config['csv_dir'], f"12-scans_by_date.csv"))
        write_to_csv(["Week","Scans"],sorted(weekly_scan_counts.items()), os.path.join(csv_config['csv_dir'], f"13-scans_by_week.csv"))

    if excel_config['enabled']:
        write_to_excel(sorted(daily_scan_counts.items()), "AW", 4)
        write_to_excel(sorted(weekly_scan_counts.items()), "AZ", 4)

    print("")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process scans and output CSV files if requested.")
    parser.add_argument("input_file", type=str, help="The JSON file containing scan data.")
    parser.add_argument("--csv", action="store_true", help="Generate CSV output files.")
    parser.add_argument("--full_data", action="store_true", help="Generate CSV output of complete scan data.")
    parser.add_argument("--name", type=str, default="", help="Optional name for the output directory")
    parser.add_argument("--excel", type=str, default="", help="Export EHC data directly to the specified Excel workbook")

    args = parser.parse_args()
    input_file = args.input_file
    output_name = args.name if args.name else os.path.splitext(os.path.basename(input_file))[0]
    excel_filename = args.excel if args.excel else None
    
    # Define the output directory using the optional name if provided
    csv_dir = os.path.join(os.getcwd(), f"ehc_output_{output_name}_{datetime.now().strftime('%Y%m%d-%H%M%S')}")

    # Initialize structures to hold output configs
    full_csv = {
        'enabled': True if args.full_data else False,
        'csv_dir': csv_dir,
        'field_names': []
    }
    csv_config = {
        'enabled': True if args.csv else False,
        'csv_dir': csv_dir
    }
    excel_config = {
        'enabled': True if args.excel else False,
        'filename': excel_filename
    }
    
    # If we're exporting to Excel...
    if excel_config['enabled']:
        # Make sure we have the required libraries
        if not  xl_available:
            parser.error("--excel requires pandas and openpyxl libraries: 'pip install pandas openpyxl'")
        # Make sure the file exists
        if not os.path.isfile(excel_filename):
            print(f"Error: The file '{excel_filename}' does not exist.")
            exit(1)
        try:
            # Open the workbook
            workbook = load_workbook(excel_filename)
        except InvalidFileException:
            print(f"Error: The file '{excel_filename}' is not a valid Excel file or is corrupted.")
            exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            exit(1)

    # If we are creating any CSV files, create the output directory i
    if args.full_data or args.csv:
        try:
            # Attempt to create the directory
            os.makedirs(csv_dir, exist_ok=True)
        except PermissionError as e:
            print(f"Permission Error: {e}")
            exit(1)
        except Exception as e:
            print(f"Error creating directory: {e}")
            exit(1)

    full_csv['field_names'], scans = ingest_file(input_file)

    processed_data = process_scans(scans, full_csv)

    output_analysis(processed_data, csv_config, excel_config)

    # If we exported to Excel, save the workbook
    if(excel_config['enabled']):
        workbook.save(excel_filename)
