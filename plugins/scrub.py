import metadata as kw
import pandas as pd
import numpy as np
import re

pd.options.mode.chained_assignment=None

def is_blank(val):
    return not (val and str(val).strip())

def is_legal_rectype(val):
    return (not val and str(val).strip().upper() in ['CHG', 'NEW', 'URGENT'])

def quality_check(df):
    if df is None:
        return False, "undefined dataframe"

    # check columns first
    column_list = df.columns
    same = True

    if(len(column_list) > len(kw.extractFormalNames)):
        same = False
        diff_str = 'additional columns: ' + str(set(column_list) - set(kw.extractFormalNames))
    elif(len(column_list) < len(kw.extractFormalNames)):
        same = False
        diff_str = 'incomplete columns: ' + str(set(kw.extractFormalNames) - set(column_list))
    else: # they have equal length, but can still be different
        a_minus_b = set(kw.extractFormalNames) - set(column_list)
        b_minus_a = set(column_list) - set(kw.extractFormalNames)
        diff_set = a_minus_b | b_minus_a # join the sets
        if(len(diff_set) > 0):
            same = False
            diff_str = 'same number but different column headers: ' + str(diff_set)

    if(not same):
        return same, diff_str

    if (len(df.index) == 0):
        return False, "empty dataframe"

    quality = True
    bad_msg = ""

    clientno = 'Source System Client No.'
    groupid = 'Grouping ID'
    client_name = 'Client Name'
    record_type = 'RecordType'

    blank_clientno_df = df[df[clientno].apply(is_blank)]
    if (len(blank_clientno_df.index) > 0):
        bad_msg += "clientno column has blanks, "
        quality = False
 
    nondigit_groupid_df = df[df[groupid].map(lambda x: not str(x).isdigit())]
    if (len(nondigit_groupid_df.index) > 0):
        bad_msg += "groupid columns has non-digit and/or blanks, "
        quality = False

    blank_client_name_df = df[df[client_name].apply(is_blank)]
    if (len(blank_client_name_df.index) > 0):
        bad_msg += "client_name column has blanks, "
        quality = False

    illegal_rectype_df = df[df[record_type].apply(is_legal_rectype)]
    if (len(blank_client_name_df.index) > 0):
        bad_msg += "record_type column has illegal values, "
        quality = False

    return quality, bad_msg

def get_phone_number(phone):
    if(is_blank(phone)):
        return ''
    else:
        phone_prime = str(phone).replace(" ", "")
        num_len = len(phone_prime)
        main_num = phone_prime[0:10]

        if(num_len < 10 or not main_num.isdigit()):
            phone_prime=''

    return phone_prime

def scrub_phone_numbers(df):
    if df is None:
        return df  

    phone_types=['Phone - Residential', 'Consent-Phone Number', 'Phone - Business', 'Phone - Fax', 'Phone - Cell']

    for cellkey in phone_types:
        if cellkey in df.columns:
            df[cellkey]=df[cellkey].map(get_phone_number)

    return df

def get_date(date):
    if(is_blank(date)):
        return ''
    else:
        date_prime = str(date).replace(" ", "")
        num_len = len(date_prime)

        if(num_len != 8 or not date_prime.isdigit()):
            date_prime=''

    return date_prime


def scrub_dates(df):
    if df is None:
        return df  

    date_types=['Loyalty Date', 'Consent-NDNCL Date', 'Birth Date', 'Credit Score Consent Date', 'Policy Effective Date', 'Policy Expiry Date']

    for cellkey in date_types:
        if cellkey in df.columns:
            df[cellkey]=df[cellkey].map(get_date)

    return df


def get_nonNA_val(val):
    if(not val or str(val).strip().upper()=='NA'):
        return ''
    else:
        return str(val).strip().upper()


def scrub_NAs(df):
    if df is None:
        return df 

    general_attr=['Suffix','Deceased Indicator','Address Line 1','Address Line 2','Country Code','Phone - Alternate','Declined E-mail',
    'Preferred contact method','Emergency Contact','Best Time to Contact','Web Site Address','Organization Type','Industry Type',
    'Organization Established Date','Personal Info Consent Date','Credit Score Consent Indicator','e-News Letters',
    'Special e-News Letters for Business Owners','e-Campaigns','e-Alerts/ Reminders','Regular Mail','Marketing Consent Last Updated Date',
    'Last Maintained Date','Grace Period End Date','Relationship type','Relationship to','Membership Since Date','Membership #',
    'Member Owner Name','Member Of Member Owner Name','Membership Association','Membership End Date']

    for cellkey in general_attr:
        if cellkey in df.columns:
            df[cellkey]=df[cellkey].map(get_nonNA_val)

    return df


def get_preset_val(val, key):
    if(not val or not key):
        return ''

    val_prime=str(val).strip().upper()
    if(key in kw.extract_check_dict and val_prime in kw.extract_check_dict[key]):
        return val_prime

    return ''

def scrub_preset_attr(df):
    if df is None:
        return df 

    preset_attr=['Prefix', 'Suffix', 'Province', 'Country', 'Preferred Langugae', 'Gender', 'Marital Status', 'Policy Province', 
    'Personal Info Consent', 'VIP', 'Credit Score Consent Indicator', 'Personal Info Consent', 'Preferred Language', 
    'Gender', 'Marital Status', 'RecordType', 'Credit Consent', 'Relationship type', 'Province/State Code', 'Occupation']

    for cellkey in preset_attr:
        if cellkey in df.columns:
            df[cellkey]=df[cellkey].apply(get_preset_val, key=cellkey)

    return df

# Rule 1 error check: The [grouping ID] must be the same, and be part of the [Source System Client No.]
def scrub_clientno_x_grpid(df):
    if df is None:
        return df

    if('Error' not in df.columns):
        df['Error']=''

    if('bad_rec' not in df.columns):
        df['bad_rec'] ='0'  # by default everything is good or bad_rec=0      

    clientno='Source System Client No.'
    grpid='Grouping ID'

    if(clientno in df.columns and grpid in df.columns):
        df['match'] = df.apply(lambda x: str(x[grpid]) in str(x[clientno]), axis=1)
        df.loc[df['match'] == False, 'Error'] = 'Inconsistent Grouping ID; ' + df['Error']
        df['bad_rec'] = np.where(df['match'] == False, 1, df['bad_rec'])

    return df


# Rule 19 error check: [Relationship to] has different values within rows of the same [Source System Client No.]
def scrub_clientno_x_relation(df):
    if df is None:
        return df
    
    if('Error' not in df.columns):
        df['Error']=''

    if('bad_rec' not in df.columns):
        df['bad_rec'] ='0'  # by default everything is good or bad_rec=0           

    clientno='Source System Client No.'
    rel_to='Relationship to'

    if((clientno in df.columns) and (rel_to in df.columns)):
        df['match'] = df.apply(lambda x: str(x[rel_to]).strip().upper()==str(x[clientno]).strip().upper(), axis=1)
        df.loc[df['match'] == True, 'Error'] = 'More than one relationships identified on this record; ' + df['Error']
        df['bad_rec'] = np.where(df['match'] == True, 1, df['bad_rec'])

    return df

def scrub_vip(df):
    if df is None:
        return df
    
    if('Warn' not in df.columns):
        df['Warn']=''

    if('warn_rec' not in df.columns):
        df['warn_rec'] ='0'  # by default everything is good or warn_rec=0           

    vip='VIP'    
    vip_contact_rep='VIP Contact Rep'
    vip_reason='VIP Reason'

    if((vip in df.columns) or (vip_contact_rep in df.columns) or (vip_reason in df.columns)):
        df['match'] = df.apply(lambda x: not(is_blank(x[vip]) and is_blank(x[vip_contact_rep]) and is_blank(x[vip_reason])), axis=1)
        df.loc[df['match'] == True, 'Warn'] = 'VIP Client Record; ' + df['Warn']
        df['warn_rec'] = np.where(df['match'] == True, 1, df['warn_rec'])   

    return df

def is_valid_clientno(clientno, reg_exp):
    return bool(reg_exp.match(clientno))

def scrub_clientno(df):
    if df is None:
        return df
        
    if('Error' not in df.columns):
        df['Error']=''

    if('bad_rec' not in df.columns):
        df['bad_rec'] ='0'  # by default everything is good or bad_rec=0           

    clientno='Source System Client No.'
    # valid client_no are of the format with possible leading/trailing spaces: 0000500908_02  
    r = re.compile(r'^\s*([0-9]+_[0-9]+)\s*$')       

    if(clientno in df.columns):
        df['match'] = df[clientno].apply(is_valid_clientno, reg_exp=r)
        df.loc[df['match'] == False, 'Error'] = 'Invalid clientno; ' + df['Error']
        df['bad_rec'] = np.where(df['match'] == False, 1, df['bad_rec'])

    return df

def scrub(df):
    scrubbed_df = scrub_clientno(df)
    scrubbed_df = scrub_NAs(scrubbed_df)
    scrubbed_df = scrub_phone_numbers(scrubbed_df)
    scrubbed_df = scrub_dates(scrubbed_df)
    scrubbed_df = scrub_preset_attr(scrubbed_df)
    scrubbed_df = scrub_clientno_x_grpid(scrubbed_df)
    # scrubbed_df = scrub_clientno_x_relation(scrubbed_df)
    scrubbed_df = scrub_vip(scrubbed_df)    
    return scrubbed_df
