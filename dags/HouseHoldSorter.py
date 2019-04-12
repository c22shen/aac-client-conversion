from io import TextIOWrapper 
import os 
import csv
import tempfile

import metadata as kw
from HouseHoldRow import HouseHoldRow
from ClientConverter import ClientConverter

class HouseHoldSorter(ClientConverter):

    def __init__( self, source_rows=[], result_rows=[] ):
        '''This is the extract part of the ETL process.  '''
        self.before_and_after_data = []

        if source_rows and isinstance(source_rows, list):
            if result_rows and isinstance(result_rows, list):
                if len(source_rows) == len(result_rows):
                    counter = 0
                    for i in range(len(source_rows)):
                        # assuming list here, but strings work too
                        source_data_raw = source_rows[i]
                        result_data_raw = result_rows[i]
                        if isinstance( source_data_raw, str ):
                            row = source_data_raw.strip()
                            source_data_raw = row.split(',')

                        if isinstance( result_data_raw, str ):
                            row = result_data_raw.strip()
                            result_data_raw = row.split(',')

                        self.before_and_after_data.append(HouseHoldRow(source_data_raw, result_data_raw))



        
    def update_households( self ):
        ''' return a list of strings of the partyLoad data '''
        self.transform()
        return_list_of_strings = []
        for row_list in self.load():
            return_list_of_strings.append( ','.join(row_list))

        return return_list_of_strings



    def transform( self ):
        '''Sort destination rows based on address and policy.  Address's of all grouping_ids much match, so 
        overwrite afer sorting'''
        self.before_and_after_data = sorted(self.before_and_after_data)
        self.before_and_after_data.reverse()


        # address's of the master
        # iterate through the list, and put the head of the house holds address in for others
        columns_to_set = [ 'city', 'address_line_1', 'address_line_2', 'province_state_code', 'country_code' ]
        # now just give me the integers of those rows
        columns_to_set = list(map( lambda x: kw.cifColKey[x], columns_to_set ))

        # current master of the house, the one whose address should overwrite other household members.
        cmoth = HouseHoldRow( [''] * len(kw.extractColNames), [''] * len(kw.cifColNames ))

        for current_row in self.before_and_after_data:
            if cmoth.in_same_household(current_row):
                for col in columns_to_set:
                    current_row.result_data[col] = cmoth.result_data[col]
            else:
                cmoth = current_row


    def load( self ):
        '''Return the array that does NOT have change records'''
        result_items = []

        for item in self.before_and_after_data:
            result_items.append(item.load())

        return result_items



