from metadata import extractColKey
from metadata import cifColKey
from ClientRow import ClientRow

class HouseHoldRow(ClientRow):
    #class variables
    policy_role_ranking_dict = {
        'HOUSEHOLDMEMBER' : 5,
        'PROPERTYOWNER'   : 4,
        'REGISTEREDOWNER' : 3,
        'THIRDPARTYPAYOR' : 2,
        'DRIVER'          : 1
    }
    
    def __init__( self, src_row=[], dest_row=[] ):
        '''This is like the extract part of the ETL process'''
        self.result_data = []
        self.source_data = []
        if src_row and isinstance( src_row, list):
            self.source_data = src_row
        if dest_row and isinstance( dest_row, list):
            self.result_data = dest_row

        self.grouping_id = str(self.source_data[extractColKey['grouping_id']]).strip().upper()
        self.policy_role = str(self.source_data[extractColKey['policy_role']]).strip().upper()
        #if self.policy_role == 'NONE':
        #    self.policy_role = ''
        self.policy_score = 0
        if self.policy_role in HouseHoldRow.policy_role_ranking_dict.keys():
            self.policy_score = HouseHoldRow.policy_role_ranking_dict[self.policy_role]


    def transform(self):
        # This is transformed in the declaration
        pass

    def in_same_household(self, other):
        return self.grouping_id == other.grouping_id 

    def details(self):
        return_str = str(self) + 'HH dets:'
        columns = [ 'city', 'address_line_1', 'address_line_2', 'province_state_code', 'country_code' ]
        for col in columns:
            return_str += col + ' = ' + self.result_data[cifColKey[col]] + ', '
        return '[{ ' + return_str + '}]\n'
    ###
    ### Rich Comparisons
    ###
    # Implement >, >=, ==, !=, <, <= functions.
    # First by grouping ID, then by policy role

    def __lt__(self, other):
        if self.grouping_id < other.grouping_id:
            return True
        elif self.grouping_id > other.grouping_id:
            return False

        return self.policy_score < other.policy_score

    def __ge__(self, other):
        return not self.__lt__(other)


    def __eq__(self, other):
        return self.grouping_id == other.grouping_id and self.policy_score == other.policy_score

    def __ne__(self, other):
        return not self.__eq__(other)


    def __le__(self, other):
        if self == other:
            return True
        return self.__lt__(other)

    def __gt__(self, other):
        return not self.__le__(other)

    def __str__(self):
        return '(HHR gid: ' + self.grouping_id + ' role: ' + self.policy_role + '[' + str(self.policy_score) + '])'

    def load(self):
        return self.result_data
