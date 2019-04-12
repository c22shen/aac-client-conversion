from io import TextIOWrapper
import os
import sys
import csv
import tempfile
import warnings

import metadata
import config
from datetime import date

from ChangeRow import ChangeRow
from ClientConverter import ClientConverter

class ChangeConverter(ClientConverter):

    def __init__( self, source_rows ):
        '''This is the extract part of the ETL process.  '''
        # source row is an array of strings, comma seperated strings.
        self.source_array = []
        if source_rows and isinstance(source_rows, list):
            for row in source_rows:
                # TODO: this should not be the case, we should pick one format for the data.
                # leaning towards the latter, ie. isinstance(row,list)
                if isinstance( row, str ):
                    row = row.strip()
                    self.source_array.append(row.split(','))
                else:
                    self.source_array.append(row)

        self.filtered_array = []
        self.destination_array = []


    def filter_out_change_records( self, change_file_name ):
        ''' transform the loaded records, filtering out CHG recs and writing them to a file.  Returns a list of strings'''
        self.transform()
        if self.are_there_change_records():
            self.write_change_file( change_file_name )
        else:
            try:

                cfn_filehandle = open( cfn, 'w', encoding='utf-8', newline='' )
                cfn_csv_writer = csv.writer( cfn_filehandle )

                cfn_csv_writer.writerow( metadata.cifColNames )
            except Exception as e:
                warnings.warn('Change file failed to write, but it was empty')
        return_list_of_strings = []
        for row in self.load_downstream_data():
            return_list_of_strings.append( ','.join(row))

        return return_list_of_strings


    def transform( self ):
        '''Filter out rows from the source array and find change records.  If they are there, filter them out, and return the NON-chnage records'''
        for row in self.source_array:
            change_row = ChangeRow(row)
            change_row.transform()
            if change_row.is_this_a_change_row():
                self.filtered_array.append(change_row.as_list())
            else:
                self.destination_array.append(change_row.as_list())


    def are_there_change_records( self ):
        return len(self.filtered_array) > 0

    def write_change_file( self, change_file_name ):
        if not change_file_name: return
        cfn = str( change_file_name )
        cfn_filehandle = None
        if not os.path.exists( cfn ):
            print( 'could not write to file: ' + cfn )
            try:
                cfn_filehandle = open( cfn, 'w', encoding='utf-8', newline='' )
            except:
                pprefix = config.change_file_prefix + '.' + date.today().strftime('%Y%m%d') + '.'
                tf = tempfile.NamedTemporaryFile(delete=False, dir='c:\\temp', prefix=pprefix,
                        suffix=config.change_file_suffix, mode='w')
                cfn = tf.name
                print( 'BUT will write to file: ' + cfn )

        try:
            cfn_filehandle = open( cfn, 'w', encoding='utf-8', newline='' )
            cfn_csv_writer = csv.writer( cfn_filehandle )

            cfn_csv_writer.writerow( metadata.cifColNames )
            for row in self.filtered_array:
                cfn_csv_writer.writerow( row )

            cfn_filehandle.close()
        except Exception as e:
            warnings.warn(str(e))
            sys.stderr.write('Failed to write ' + len(self.filtered_array) + ' Extract Change Records to File: ' + cfn)

