import timeit
import sys
import pandas as pd
import warnings
import re
import argparse
import os

from datetime import date

import metadata as kw
import config

from ChangeConverter import ChangeConverter
from HouseHoldSorter import HouseHoldSorter

import aicollapse
import scrub
import businessRules
''' mapping of coseco extract columns and values to a CIF party load file'''

def get_party_load_name_from_input_file_name(extract_input_file, force=None):
    # given: path/to/file.name.yyyymmdd.thing.csv  gives you file.name.yyyymmdd.thing
    if extract_input_file and isinstance(extract_input_file, str):
        file_basename = os.path.splitext(os.path.basename(extract_input_file))[0]
        if 'regular' in file_basename:
            reg_or_urg = 'regular'
        elif 'urgent' in file_basename:
            reg_or_urg = 'urgent'
        else:
            reg_or_urg = file_basename

        
        eight_digit_search = re.search('\d\d\d\d\d\d\d\d',extract_input_file)

        if force:
            final_result = reg_or_urg + '.' + force
            return final_result

        if eight_digit_search != None:
            final_result = reg_or_urg + '.' + eight_digit_search.group(0) 
            if eight_digit_search.group(0) in reg_or_urg:
                final_result = reg_or_urg
            return final_result

        final_result = file_basename + '.' + date.today().strftime('%Y%m%d')
        return final_result
    else:
        date.today().strftime('%Y%m%d')
        return date.today().strftime('%Y%m%d')

def python_execute(extract_input_file):
    # define file names
    party_load_name = get_party_load_name_from_input_file_name(extract_input_file)
    extract_warning_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.warning.csv'
    extract_error_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.error.csv'
    extract_scrub_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.scrub.csv'
    extract_collapsed_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.collapsed.csv'
    extract_changed_records_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.chg.csv'
    extract_group_rpas_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.RPAgroup.csv'
    extract_group_momo_filename = config.output_folder + 'ECMExtract.' + party_load_name + '.RPAmomo.csv'

    party_load_output_filename = config.output_folder + 'PARTY.' + party_load_name + '.load.csv'
    party_load_output_error_filename = config.output_folder + 'PARTY.' + party_load_name + '.error.csv'
    party_load_output_warning_filename = config.output_folder + 'PARTY.' + party_load_name + '.warning.csv'
    
    # Read csv into dataframe 
    raw_extract_data_df = None
    try:
        raw_extract_data_df = pd.read_csv(extract_input_file, keep_default_na=False, encoding='latin-1', skip_blank_lines=True)
    except Exception as err:
        sys.stderr.write('Failed to open file: ' + extract_input_file)
        sys.stderr.write(str(err) + '\n')
        sys.exit(1)
    
    # Generate post-cleaning files
    scrubbed_df = scrub.scrub(raw_extract_data_df)

    extract_warn_records_df = scrubbed_df[scrubbed_df['warn_rec'] == 1] 
    extract_warn_records_df.drop(columns=['Error', 'match', 'bad_rec', 'warn_rec'], inplace=True)

    extract_errors_records_df = scrubbed_df[scrubbed_df['bad_rec'] == 1]
    extract_errors_records_df.drop(columns=['Warn', 'match', 'bad_rec', 'warn_rec'], inplace=True)

    extract_records_df = scrubbed_df[scrubbed_df['bad_rec'] != 1]
    extract_records_df.drop(columns=['Error', 'Warn', 'match', 'bad_rec', 'warn_rec'], inplace=True)
    
    extract_records_df.to_csv(extract_scrub_filename ,index=False)

    # Collapse files
    # TODO: enable aiCollapseWtRules to directly on scrubbed data containing warnings and errors
    groupBy='Source System Client No.'
    string_comparison_threshold=82
    numExtractRecords, collapsed_df, err_recs_df, warn_df =aicollapse.aiCollapseWtRules(extract_records_df,groupBy, string_comparison_threshold)    

    collapsed_df.to_csv(extract_collapsed_filename ,index=False)

    extract_errors_records_df = extract_errors_records_df.append(err_recs_df, ignore_index=True)
    extract_errors_records_df.to_csv(extract_error_filename,index=False)

    extract_warn_records_df = extract_warn_records_df.append(warn_df, ignore_index=True)
    extract_warn_records_df.to_csv(extract_warning_filename,index=False)     

    # RPA Files 
    group_rpa_df = extract_records_df[extract_records_df.apply(lambda x: not(scrub.is_blank(x['Group Name']) 
        and scrub.is_blank(x['Group Number']) and scrub.is_blank(x['Office No']) and scrub.is_blank(x['Office Name'])), axis=1)]
    group_rpa_df.to_csv(extract_group_rpas_filename ,index=False)
    
    momo_df = extract_records_df[extract_records_df['Grouping ID'].isin(kw.momo_rpa)]
    momo_df.to_csv(extract_group_momo_filename ,index=False)

    extract_records = extract_records_df.astype(str).values.tolist()
    numExtractRecords = len(extract_records)

    #Change Records
    change_filter = ChangeConverter(extract_records.copy())
    extract_records = change_filter.filter_out_change_records(extract_changed_records_filename)

    # Apply Businessrules 
    counter, party_load_records, missingColSet, skips = businessRules.businessRules(extract_records)

    # Sorting Logic
    address_sorter = HouseHoldSorter( extract_records, party_load_records )
    party_load_records = address_sorter.update_households()

    # Generate party load output file
    output = open(party_load_output_filename , 'w') 
    output.write(','.join(kw.cifFormalName) + '\n' )

    for r in party_load_records:
        output.write(r)
        output.write("\n")
    output.close()

    #Generate Party Error File
    output = open(party_load_output_error_filename , 'w')
    # write an extra column for the error message, hence the comma in the line below
    output.write(','.join(kw.cifFormalName) +',\n' )
    # TODO: please verify missingColSet is the correct item here
    for row in missingColSet:
        output.write(row)
        output.write("\n")
    output.close()

    #Generate Party Warning File 
    output = open(party_load_output_warning_filename, 'w')
    # write an extra column for the error message, hence the comma in the line below
    output.write(','.join(kw.cifFormalName) + ',\n' )
    # TODO: where is the source of this data?
    #    for row in something_to_loop
    output.close()

if __name__ == '__main__':
    python_execute('ECMExtract.DB2Data20190412.regular.csv')


