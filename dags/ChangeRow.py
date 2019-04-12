from metadata import extractColKey
from ClientRow import ClientRow

class ChangeRow(ClientRow):
    
    def __init__( self, src_row=[] ):
        super().__init__(src_row)


    def is_this_a_change_row(self):
        '''there is only one field, the "recordType" that determines this'''
        if int(extractColKey['recordtype']) < len(self.source_data):
            rec_type = str(self.source_data[extractColKey['recordtype']]).strip().upper()
            return rec_type == 'CHG'
        return False

