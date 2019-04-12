

class ClientRow:
    ''' This is the base class of the Objects used in the project'''


    def __init__( self, src_row=[] ):
        '''This is like the extract part of the ETL process'''
        self.source_data = []
        self.result_data = []
        if src_row and isinstance( src_row, list ):
            self.source_data = src_row


    def transform(self):
        '''This is the transform part of the ETL process'''
        self.result_data = self.source_data



    def load(self):
        '''Give the result of the transformation'''
        return self.result_data
    
    def __str__(self):
        '''Give the result of the transformation'''
        return ', '.join( self.result_data )
    
    def as_list(self):
        return list(self.result_data)
