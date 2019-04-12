import os

from io import TextIOWrapper

from ClientRow import ClientRow

class ClientConverter:
    ''' This is the base class of the Objects used in the project'''

    def __init__( self, source_rows ):
        '''This is the extract part of the ETL process.  '''
        self.source_array = []
        if source_rows and isinstance(source_rows, list):
            self.source_array = source_rows

        self.destination_array = []


    def load_downstream_data( self ):
        return self.destination_array

    def __del__( self ):
        pass

