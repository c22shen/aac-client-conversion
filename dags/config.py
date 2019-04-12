
# Will look for 2 files, both starting the same but ending differently.
# prefix + YYYYMMDD + [ suffix ] + ".csv"
extract_source_file_suffixes = [ 'urgent', 'regular' ]
#extract_source_file_location = r'i:\\dryRuns\\'
extract_source_file_location = r'c:\\CPOC\\cpoc-ai-ml\\'
destination_directory = extract_source_file_location




extract_source_file_prefix = 'ECMExtract.DB2Data'


# prefix + YYYYMMDD + extract_suffix + suffix + ".csv"
change_file_prefix = 'ECMExtract'
change_file_suffix = 'chg.csv'

output_folder = './'
# output_folder = 'AI_Output/'
input_folder = 'AI_Input/'

party_load_prefix = extract_source_file_prefix
party_load_suffix = 'PartyLoad'


# these are actually used in the scripts
party_load_file_prefix = 'PARTY.'
inbetween_file_prefix = 'ECMExtract.'
party_load_file_suffix = '.load.csv'

