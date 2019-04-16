import timeit
import sys
import re

import metadata as kw

import datetime
from datetime import datetime as dt



# aac-478
def get_province_state_code_from_row(source_row):
    if source_row and isinstance(source_row, list):
        if int(kw.extractColKey['province_state_code']) < len(source_row):
            code = str(source_row[kw.extractColKey['province_state_code']]).strip().upper()
            if not code == 'NONE':
                if code in kw.province_code_map: 
                    return kw.province_code_map[code]
    return ''

#aac-495
def get_follow_up_date_from_row(source_row):
    if source_row and int(kw.extractColKey['follow_up_date']) < len(source_row):
        date = str(source_row[kw.extractColKey['follow_up_date']]).strip()
        if not date == 'None' and len(date) == 8 and date.isdigit():
                return format_date(date)
    return ''


# aac-491
# hardcoded values
def get_personal_info_consent():
    return 'Y'
def get_coseco_admin_system_type():
    return 'AS400_COSECO_CLIENT'


# aac-480
def get_country_code_from_result_dict(source_row):
    if not source_row:
        return ''
    if not kw.extractColKey['province_state_code'] < len(kw.extractColNames):
        return ''
    extract_country_code = str(source_row[kw.extractColKey['province_state_code']]).strip().upper()
    if extract_country_code in kw.country_code_extract_to_party_load_map:
        return kw.country_code_extract_to_party_load_map[extract_country_code]
    return ''


# aac-498
def get_prefix_from_row_and_results(source_row, destination_dict):
    if not destination_dict or not source_row:
        return ''
    if not 'person_org_indicator' in destination_dict:
        return ''
    po_ind = str(destination_dict['person_org_indicator']).strip().upper()
    if po_ind == 'YES':
        return ''
    if not kw.extractColKey['prefix'] < len(source_row):
        return ''
    prefix = str(source_row[kw.extractColKey['prefix']]).strip().upper()
    if prefix == None:
        return ''
    if prefix in kw.prefix_extract_to_party_load_map:
        return kw.prefix_extract_to_party_load_map[prefix]

    #aac608
    # find prefixes in names
    first_name = str(source_row[kw.extractColKey['first_name']]).strip().upper()
    middle_name = str(source_row[kw.extractColKey['middle_name']]).strip().upper()
    last_name = str(source_row[kw.extractColKey['last_name']]).strip().upper()
    for prefix in kw.prefix_extract_to_party_load_map:
        look_for = prefix + ' '
        length = len(look_for)
        if first_name[0:length] == look_for:
            return kw.prefix_extract_to_party_load_map[prefix]
        if middle_name[0:length] == look_for:
            return kw.prefix_extract_to_party_load_map[prefix]
        if last_name[0:length] == look_for:
            return kw.prefix_extract_to_party_load_map[prefix]


    #/aac608
    return ''

# AAC-486
def get_organization_type_from_destination_row(destination_dict):
    if not destination_dict:
        return '25'

    person_org_indicator = "YES"
    if 'person_org_indicator' in destination_dict:
        person_org_indicator = str(destination_dict['person_org_indicator']).upper()

    organization_name = 'None'
    if 'organization_name' in destination_dict:
        organization_name = str(destination_dict['organization_name'])

    if person_org_indicator == 'NO':
        return ''
    else:
        if re.search('\&', organization_name):
            return '26'
        else:
            return '25'



# transformation rules

#aac-473
def get_primary_email_from_row(extract_row):
    if not extract_row:
        return ''
    elif not int(kw.extractColKey['web_site_address']) < len(extract_row):
        return ''
    else:
        string = str(extract_row[kw.extractColKey['e_mail_address_primary']]).strip()
        if string == 'None':
            return ''
        string_prime = re.sub( '\s\s*', '', string)
        if ';' in string_prime:
            return string_prime[0:string_prime.index(';')]

        return string_prime

# AAC-485
def get_web_site_address_from_row(extract_row):
    if not extract_row:
        return ''
    elif not int(kw.extractColKey['web_site_address']) < len(extract_row):
        return ''
    else:
        web_site_address = str(extract_row[kw.extractColKey['web_site_address']]).strip()
        if web_site_address == 'None':
            return ''
        return re.sub( '\s\s*', '', web_site_address)

#########
#aac-489
def is_the_result_dict_an_organization(results_dict):
    if not results_dict:
        return False
    if 'person_org_indicator' in results_dict and results_dict['person_org_indicator'].upper().strip() == 'YES':
        return True
    return False

def get_marital_status_from_source_and_results(source_row, results_dict):
    if is_the_result_dict_an_organization(results_dict):
        return ''
    elif not source_row or not isinstance( source_row, list ):
        return '3'
    else:
        source_value = str(source_row[kw.extractColKey['marital_status']]).strip().upper()
        if source_value == 'M':
            return '1'
        elif source_value == 'S':
            return '2'
        return '3'

#########


##########################
##########################
# AAC-468 
def is_the_row_an_organization(source_row):
    ''' FALSE = a human, TRUE = a corporation.  CURRENTLY ALL INPUTS are hardcoded as "NO" '''
    try:
        value = source_row[kw.extractColKey['organization_type']].strip().upper()
        if value:
            return True
        return False
    except:
        return False
    
# updated as part of this ticket (birth_date)
def format_date(source_date):
    ''' Takes in a string assumed to be YYYYMMDD and returns YYYY-MM-DD.  
        Input can be YYYYMMDDYYYYMMDD and the earliest date will be returned'''
    return_date = ""

    if source_date:
        number_of_8_digit_dates_in_the_string = int(len(source_date)/8)
        selected_date = source_date

        if number_of_8_digit_dates_in_the_string == 2:
            date1_str = source_date[0:8]
            d1 = datetime.datetime.strptime(date1_str,"%Y%m%d")
            date2_str = source_date[8:16]
            d2 = datetime.datetime.strptime(date2_str,"%Y%m%d")
            if(d1 < d2):
                selected_date = date1_str
            else:
                selected_date = date2_str

        yyyy = selected_date[0:4]
        mm = selected_date[4:6]
        dd = selected_date[6:8]
        return_date = yyyy + "-" + mm + "-" + dd
    return return_date


# aac-488
def get_occupation_code_from_row(source_row, destination_dict):
    if not source_row or not destination_dict:
        return '14'
    elif not 'person_org_indicator' in destination_dict:
        return '14'
    elif not int(kw.extractColKey['occupation']) < len(source_row):
        return '14'
    elif not int(kw.extractColKey['province_state_code']) < len(source_row):
        return '14'
    else:
        if is_the_result_dict_an_organization(destination_dict):
            return '14'
        else:
            province_state = str(source_row[kw.extractColKey['province_state_code']]).strip().upper()
            occupation = str(source_row[kw.extractColKey['occupation']]).strip().upper()
            if province_state == 'NONE' or occupation == 'NONE':
                return '14'
            if province_state == 'PQ':
                if occupation in kw.occupation_string_to_party_load_code_map_for_pq_province:
                    return kw.occupation_string_to_party_load_code_map_for_pq_province[occupation]
            else:
                if occupation in kw.occupation_string_to_party_load_code_map:
                    return kw.occupation_string_to_party_load_code_map[occupation]
            return '14'


## reformating dates
def formatDate(cosecoFormat):
    date=""

    if(cosecoFormat):
        numOfDates=len(cosecoFormat)/8
        selected=cosecoFormat

    if(numOfDates==2):
        d1_str=cosecoFormat[0:8]
        d1=dt.strptime(d1_str,"%Y%m%d")
        d2_str=cosecoFormat[8:16]
        d2=dt.strptime(d2_str,"%Y%m%d")
        if(d1 < d2):
            selected=d1_str
        else:
            selected=d2_str

    yyyy=selected[0:4]
    mm=selected[4:6]
    dd=selected[6:8]
    date=yyyy + "-" + mm + "-" + dd
    return date

def is_the_row_an_organization(destination_row):
    ''' FALSE = a human, TRUE = a corporation.  CURRENTLY ALL INPUTS are hardcoded as "NO" '''
    result = False
    if not destination_row:
        return result

    if 'organization_name' in destination_row:
        if destination_row['organization_name']:
            result = True
    return result
    
# updated as part of this ticket (birth_date)
def format_date(source_date):
    ''' Takes in a string assumed to be YYYYMMDD and returns YYYY-MM-DD.  
        Input can be YYYYMMDDYYYYMMDD and the earliest date will be returned'''
    return_date = ""

    if source_date:
        number_of_8_digit_dates_in_the_string = int(len(source_date)/8)
        selected_date = source_date

        if number_of_8_digit_dates_in_the_string == 2:
            date1_str = source_date[0:8]
            d1 = datetime.datetime.strptime(date1_str,"%Y%m%d")
            date2_str = source_date[8:16]
            d2 = datetime.datetime.strptime(date2_str,"%Y%m%d")
            if(d1 < d2):
                selected_date = date1_str
            else:
                selected_date = date2_str

        yyyy = selected_date[0:4]
        mm = selected_date[4:6]
        dd = selected_date[6:8]
        return_date = yyyy + "-" + mm + "-" + dd
    return return_date


# aac-484
def get_emergency_contact_from_row(source_row):
    return give_empty_or_value_from_extract_row(source_row, int(kw.extractColKey['emergency_contact']))

def get_best_time_to_call_from_row(source_row):
    return give_empty_or_value_from_extract_row(source_row, int(kw.extractColKey['best_time_to_contact']))

def give_empty_or_value_from_extract_row(extract_row, extract_source_index=0):
    if not extract_row:
        return ''
    elif extract_source_index < len(extract_row):
        best_time_value = str(extract_row[extract_source_index]).strip()
        compare_value = best_time_value.upper()
        if compare_value == 'NONE' or compare_value == 'NA':
            return ''
        return best_time_value
    else:
        return ''

def get_birth_date_from_row(source_row, result_dictionary=None):
    if is_the_row_an_organization(result_dictionary):
        return ''
    else:
        source_value = source_row[kw.extractColKey['birth_date']].strip()
        only_digits_check = re.compile('^(\d{8}|\d{16})$')
        if only_digits_check.search(source_value):
            try:
                formatted_date = dt.date.fromisoformat( format_date(source_value) )
            except ValueError:
                # In this case, a day or month is out of range (example march 34th)
                return ''
            today = dt.date.today()
            if formatted_date <= today:
                return formatted_date.isoformat()
            else:
                return ''
        else:
            # TODO: getting the birthdate from the drivers license would go here
            return ''


##
def get_deceased_indicator(person_org_indicator, input_row_list):
    if(person_org_indicator.strip().upper()=="YES"):
        return ""

    if input_row_list:
        fields_to_check = [ input_row_list[kw.extractColKey['first_name']],
            input_row_list[kw.extractColKey['last_name']],
            input_row_list[kw.extractColKey['middle_name']] ]
        for field in fields_to_check:
            if field:
                field_prime=str(field).strip().upper()
                if "ESTATE" in field_prime:
                    return "Y"
    return 'N'


def realtEmail(altEmail):
	if(altEmail and altEmail == "See other list"):
		altEmail=""	
	return altEmail


## aac-617
def get_declined_email(tokens, recordDict):
    declined_email = str(tokens[kw.extractColKey['declined_e_mail']]).strip().upper()
    email_address = str(recordDict['e_mail_address_primary'])
    email_address_alternate = str(recordDict['e_mail_address_alternate'])

    if declined_email == 'N':
        return ''
    elif declined_email == 'Y' or email_address or email_address_alternate:
        return 'Y'
    return ''


# AAC-465
def translate_organization_name_from_row( client_row ):
    if not client_row:
        return ''
    elif not int(kw.extractColKey['client_name']) < len(client_row):
        #element does not exist
        return ''
    else:
        client_name = str(client_row[kw.extractColKey['client_name']]).strip()

        if client_name == 'None' or not client_name:
            return ''
        
        for keyword in kw.organization_name_keywords:
            if keyword.upper() in client_name.upper():
                return client_name

        at_least_two_comma_search = re.search('^[^,]*,[^,]*,', client_name)
        if at_least_two_comma_search:
            return client_name

        # 1 comma available, and the length of the delimited string ALL have length >= 2
        #    ABC DEF, GHI JKL -> Yes
        #    ABC, GHI JKL DEF -> No

        single_comma_search = re.search('^[^,]+,[^,]*$', client_name)
        if single_comma_search:
            for section in client_name.split(','):
                if len(re.split( r'\W+', section.strip())) <= 1:
                    return ''
            return client_name

        return ''

def get_care_of_from_row(input_row):
    if not input_row:
        return ''
    elif not int(kw.extractColKey['care_of']) < len( input_row ):
        return ''
    elif not input_row[kw.extractColKey['care_of']] or not isinstance(input_row[kw.extractColKey['care_of']], str): 
        return ''

    care_of_value = str(input_row[kw.extractColKey['care_of']]).strip().upper()
    if care_of_value == 'NA':
        return ''
    return care_of_value 


## 
def reprefix(prefix):
	if(prefix == "2"):
		prefix="27"
	elif(prefix == "1"):
		prefix="26"
	elif(prefix == "3"):
		prefix="25"
	elif(prefix == "5"):
		prefix="21"
	return prefix

def repostalCode(val):
    if(not val):
        return ""

    val_prime = str(val).strip()
    val_prime_len = len(val_prime)
    if(val_prime_len < 5 or val_prime_len > 9):
        return ""

    if(val_prime_len == 6):
        first3=(val_prime[0:3]).upper()
        last3=(val_prime[3:6]).upper()
        return first3 + " " + last3
    
    # any other case, return val asis
    return val_prime    

##
def rewed(wed):
	if(wed == "M"):
		wed="1"
	elif(wed == "S"):
		wed="2"
	else:
		wed="3"
	return wed

#
## AAC-353
#
def get_language_from_row(source_row):
    if not source_row:
        return '100'
    original_language = str(source_row[kw.extractColKey['preferred_language']]).strip().upper()
    if original_language == 'F':
        return '200'
    else:
        return '100'

##
def relang(lang):
	if(lang == "E"):
		lang="100"
	elif(lang == "F"):
		lang="200"
	else:
		lang="000"
	return lang	

##
def repersonalConsent(pc):
	if(pc and pc == "0"):
		pc="Y"
	return pc

##
def recountryCode(countryCode):
	if(countryCode and countryCode=="NA"):
		countryCode="CA"
	return countryCode

##
def recredit_consent(creditConsent, person_org_indicator):
    if(person_org_indicator and person_org_indicator=="YES"):
        return ''

    ret='U'
    if(creditConsent and creditConsent!=0):
        creditConsent_prime=creditConsent.strip().upper()
        if(creditConsent_prime=='Y' or creditConsent_prime=='N'):
            ret=creditConsent_prime

    return ret


## reformating dates
def formatDate(cosecoFormat):
    if(not cosecoFormat or cosecoFormat==0 or cosecoFormat=="0" 
        or cosecoFormat.strip().upper()=="NA" or len(cosecoFormat.strip())!=8
        or not cosecoFormat.strip().isdigit()):
        return cosecoFormat, False

    date = cosecoFormat.strip()
    yyyy=date[0:4]
    mm=date[4:6]
    dd=date[6:8]
    date=yyyy + "-" + mm + "-" + dd
    return date, True


def recredit_date(credit_date, person_org_indicator):
    if(person_org_indicator and person_org_indicator=="YES"):
        return ''

    date, changed = formatDate(credit_date) 

    if(not changed):
        return dt.today().strftime("%Y-%m-%d")
    else:
        return date

##
def reeletters(eletters):
	if(not eletters or eletters == "NA"):
		eletters="U"
	return eletters

##
def reeCampaigns(eCampaigns):
	if(not eCampaigns or eCampaigns == "NA"):
		eCampaigns="U"
	return eCampaigns

##
def realerts(alerts):
	if(not alerts or alerts == "NA"):
		alerts="U"
	return alerts

##
def remail(mail):
	if(not mail or mail == "NA"):
		mail="U"
	return mail

##
def refollowDate(followDate):
	#out of scope
	return ""

##
def rephone_biz(val):
    if(not val or val==0 or val=="0" or val == "NA"):
        return ""

    val_prime = val.replace(" ", "")
    num_len = len(val_prime)
    main_num = val_prime[0:10]
    if(num_len < 10 or main_num == "1111111111" or not main_num.isdigit()):
        return ""
           
    if(num_len == 10 or num_len == 11):
        return main_num #return first 10 digits

    if(val_prime[10].upper()=="X"):
        end_of_ext=min(num_len, 11+6)
        ext = val_prime[11:end_of_ext]
        if(ext.isdigit()):
            return main_num + " X" + ext

    return main_num

##
def rephone(val):
    if(not val or val==0 or val=="0" or val == "NA" or len(val.strip()) < 10
        or val.strip()[0:10] == "1111111111" or not (val.strip()[0:10]).isdigit()):
        return ""
    
    return val.strip()[0:10] #return first 10 digits

##
def rebusinessPartnersAffiliates(val):
	if(not val or val == "NA" or val == "CBS derives"):
		val="U"
	return val


## aac-608 COMPLETELY REDONE
def get_suffix(extract, party_load):
    suffix = str(extract[kw.extractColKey['suffix']]).strip().upper()
    if is_the_result_dict_an_organization(party_load):
        return ''
    if suffix == 'NA' or suffix == 'NONE':
        return ''

    # check FN,LN,MN
    fn = str(extract[kw.extractColKey['first_name']]).strip().upper()
    ln = str(extract[kw.extractColKey['last_name']]).strip().upper()
    mn = str(extract[kw.extractColKey['middle_name']]).strip().upper()
    search_string = fn + ' ' + mn + ' ' + ln
    for potential_suffix in kw.valid_suffixes:
        look_for = ' ' + potential_suffix 
        if look_for in search_string:
            return potential_suffix.upper()

    # last resort
    return suffix

## /aac-608 COMPLETELY REDONE

def resuffix(suffix, person_org_indicator):
	suffix_prime = str(suffix).strip().upper()
	if((suffix_prime=="NA") or (person_org_indicator=="YES")):
		return ""
	else:
		return suffix_prime


def regroupKey(val):
	return "CPOC_CLIENTCONVERSION_" + str(val).strip()

##
def reperson_org_indicator(val):
	if(str(val).strip()):
		return "YES"
	else:
		return "NO"



##
def re_address_line_1(val):
    subtokens=str(val).strip().split(",")
    return subtokens[0].strip()

##
def re_address_line_2(val):
    res=""
    subtokens=str(val).strip().split(",")
    len_subtokens=len(subtokens)

    if(len(subtokens) == 1): # no comma
        res = subtokens[0].strip()
    elif(len(subtokens) > 1):
        res = subtokens[1].strip()

    return res

## assumptions: (a) loyal_date and birthday are strings
def reloyalty_date(loyal_date, birthday, person_org_indicator):
    if not loyal_date or loyal_date==0 or loyal_date=='0' or loyal_date.strip().upper()=='NA' or not person_org_indicator:
        return ""

    date=loyal_date.strip()
    loyal_date_prime=int(date)
    today=int(dt.today().strftime('%Y%m%d'))
    if(loyal_date_prime > today):
        date= str(today)

    birthday_prime = int(birthday.strip().replace("-", ""))

    if(birthday_prime and loyal_date_prime < birthday_prime):
        date = birthday        

    cifdate, changed = formatDate(date)

    return cifdate

## AAC: 487, 620
def get_gender( input_token_list,  dest_dict ):
    default='U'
    party_key='person_org_indicator'
    extract_key='gender'
    helper_key='prefix'
    driver_key='drivers_license_number'
    if not dest_dict:
        return default

    person_org_indicator = "NO"
    if party_key in dest_dict:
        person_org_indicator = str(dest_dict[party_key]).upper()    

    if(person_org_indicator == "YES"):
        return ''

    if not input_token_list:
        return default
    
    gender_prime = input_token_list[kw.extractColKey[extract_key]].strip().upper()
    if(gender_prime == '1'):
        return 'M'
    elif(gender_prime == '2'):
        return 'F'

    prefix = input_token_list[kw.extractColKey[helper_key]]
    extract_gender = ''
    if(prefix and str(prefix).strip().upper() in kw.extract_prefix_2_gender_mapping.keys()):
        extract_gender=kw.extract_prefix_2_gender_mapping[prefix.strip().upper()]           

    if(extract_gender == '1'):
        return 'M'
    elif(extract_gender == '2'):
        return 'F'

    driver_license = input_token_list[kw.extractColKey[driver_key]]
    if(driver_license):
        dl_prime = str(driver_license).strip().upper()
        if(len(dl_prime) == 15 and dl_prime[0].isalpha() 
        and dl_prime[1:15].isdigit()):
            if(int(dl_prime[-4:-2]) > 50):
                extract_gender = '2'
            else:
                extract_gender = '1'

    if(extract_gender == '1'):
        return 'M'
    elif(extract_gender == '2'):
        return 'F'

    return default


## aac-613, 612 and 610
# this is unit tested by its calling functions get_(first|mid|last)name
def _purge_company_keywords_from_string( input_str='', purge_commas=False ):
    if not input_str or not isinstance( input_str, str):
        return ''
    return_str = input_str.upper().strip()
    return_str = return_str.replace( 'ESTATE OF', '' )
    return_str = return_str.replace( 'ESTATE', '' )
    # don't remove commas, that will confuse the logic of get_first|last|middle
    if purge_commas:
        return_str = return_str.replace( ',', '' )
    return_str = return_str.replace( '\\', '' )
    return_str = return_str.replace( '/', '' )
    # open and close brackets and the contents between.  either bracket alone is fine
    return_str = re.sub('\([^\)]*\)', '', return_str)
    return_str = re.sub('\d', '', return_str)
    return_str = re.sub('\s{2,}', ' ', return_str)
    return_str = return_str.strip()
    return return_str





## AAC-500
def get_last_name( input_token_list,  dest_dict ):
    default=''
    party_key='person_org_indicator'
    main_extract_key='last_name'
    helper_extract_key='first_name'
    
    if not input_token_list:
        return default
    elif not int(kw.extractColKey[main_extract_key]) < len( input_token_list ):
        # a certain index does not exist
        return default

    if dest_dict: # if dest_dict doesn't exist; assume not company       
        person_org_indicator = "NO"
        if party_key in dest_dict:
            person_org_indicator = str(dest_dict[party_key]).upper()    

        if(person_org_indicator == "YES"):
            return default

    if is_the_result_dict_an_organization(dest_dict):
        return default


    if(input_token_list[kw.extractColKey[main_extract_key]]) and isinstance(input_token_list[kw.extractColKey[main_extract_key]], str):
        last_name_prime = input_token_list[kw.extractColKey[main_extract_key]].strip().upper()
        return _purge_company_keywords_from_string(last_name_prime, purge_commas=True)
    elif(input_token_list[kw.extractColKey[helper_extract_key]]) and isinstance(input_token_list[kw.extractColKey[helper_extract_key]], str):
        first_name_composite = input_token_list[kw.extractColKey[helper_extract_key]].strip().upper()

        csv_names=first_name_composite.split(",")
        if(len(csv_names)<2): 
            # e.g. csv_names = ['john smith']
            space_names=first_name_composite.split(" ")
            if(len(space_names)<2): 
                return default # space_names = ['john']; only one name is present; likely first name
            else: # space_names = ['john', 'F', 'smith']
                last_name_prime = space_names[len(space_names)-1]
                return _purge_company_keywords_from_string(last_name_prime, purge_commas=True)
        else: # e.g. csv_names = ['smith', 'john']
            last_name_prime = csv_names[0].strip()
            return _purge_company_keywords_from_string(last_name_prime, purge_commas=True)

    return default

def is_blank(input_token_list, col_key):
    col_val=input_token_list[kw.extractColKey[col_key]]
    return (not col_val or col_val==0 or col_val=='0' or col_val=='' or (col_val.strip().upper())=='NA')

## AAC-499
def get_first_name( input_token_list,  dest_dict ):
    default=''
    main_extract_key='first_name'
    helper_extract_key='last_name'
    party_key='person_org_indicator'

    if is_the_result_dict_an_organization(dest_dict):
        return default

    if not input_token_list:
        return default
    elif not int(kw.extractColKey[main_extract_key]) < len( input_token_list ):
        # a certain index does not exist
        return default    
    
    if(input_token_list[kw.extractColKey[main_extract_key]]) and isinstance(input_token_list[kw.extractColKey[main_extract_key]], str):
        first_name_composite = input_token_list[kw.extractColKey[main_extract_key]].strip().upper()
        if not is_the_result_dict_an_organization( dest_dict ):
            first_name_composite = _purge_company_keywords_from_string(first_name_composite)
        

        if(is_blank(input_token_list,helper_extract_key)):
            csv_names=first_name_composite.split(",")
            if(len(csv_names)<2): 
                # e.g. csv_names = ['john smith']                
                space_names=first_name_composite.split(" ")
                if(len(space_names)<2): 
                    return first_name_composite # space_names = ['john']; only one name is present; likely the only available name
                else: # space_names = ['john', 'F', 'smith']
                    first_name_prime = space_names[0]
                    return first_name_prime
            else: # e.g. csv_names = ['smith', 'john']
                first_name_prime = csv_names[len(csv_names)-1].strip()
                return first_name_prime
        else:
            return _purge_company_keywords_from_string(first_name_composite) # last_name present so first_name_composite is the first name

    return default

## AAC-510
def get_middle_name( input_token_list,  dest_dict ):
    default=''
    main_extract_key='middle_name'

    if is_the_result_dict_an_organization(dest_dict):
        return default

    if not input_token_list:
        return default
    elif not int(kw.extractColKey[main_extract_key]) < len( input_token_list ):
        # a certain index does not exist
        return default
    elif is_blank(input_token_list, main_extract_key):
        return default
    else:
        value = input_token_list[kw.extractColKey[main_extract_key]].strip().upper()
        return _purge_company_keywords_from_string( value, purge_commas=True)


##
def reblanketRule(val):
	if(val and (val == "NA" or val == "CBS derives")):
		val=""
	return val

def regroupKey(val):
	return "CPOC_CLIENTCONVERSION_" + str(val).strip()

def translate_city( input_row ):
    if not input_row:
        return ''
    elif not int(kw.extractColKey['city']) < len( input_row ):
        # a certain index does not exist
        return ''
    elif not input_row[kw.extractColKey['city']] or not isinstance(input_row[kw.extractColKey['city']], str): 
        # specific element is not a string or is None
        return ''
    return str(input_row[kw.extractColKey['city']]).strip().upper()

def businessRules(compressedRecs):
    #initialize variables
    counter=0
    abnormalRecords=0
    tolerance=1
    missingColSet=set([])
    records=[]
    skips=0

    for line in compressedRecs:
        recordDict={}
        for column_name in kw.cifColNames: #initialize
            recordDict[column_name]="999"
        val = ""
        tokens=line.split(',')
        if(len(tokens)<len(kw.extractColNames)):
            skips+=1
            print("len(tokens)<=len(kw.extractColNames))" + str(len(tokens)) + "," + str(len(kw.extractColNames)))
            continue
        counter += 1
        numTokensUsed=0
        for column_name in kw.cifColNames:
            #business rules on columns
            if(column_name == 'identifier'):
                val = str(counter)
            elif(column_name == 'person_org_indicator'):
               	org_name=recordDict['organization_name']
               	val=reperson_org_indicator(org_name)
            elif(column_name == 'coseco_admin_system_type'): # hardcoded
                val = "AS400_COSECO_CLIENT"
            elif(column_name in kw.extractColKey):
                numTokensUsed+=1
                #business rules on columns
                if(column_name == 'grouping_id'):
                    # val = "CPOC_CLIENTCONVERSION_" + tokens[kw.extractColKey[column_name]].strip()
                    val=regroupKey(tokens[kw.extractColKey[column_name]])
                elif column_name == 'loyalty_date':
                    date = tokens[kw.extractColKey[column_name]]
                    bd = recordDict['birth_date']
                    person_org_indicator = is_the_result_dict_an_organization(recordDict)
                    val = reloyalty_date(date, bd, person_org_indicator)
                elif(column_name == 'organization_name'):
                    val = translate_organization_name_from_row(tokens)
                elif column_name == 'web_site_address':
                    val = get_web_site_address_from_row(tokens)
                elif(column_name == 'birth_date'):
                    val = get_birth_date_from_row(tokens)
                elif column_name == 'suffix':
                    val = get_suffix(tokens, recordDict)
                elif(column_name == 'last_name'):
                    val = get_last_name(tokens, recordDict)
                elif(column_name == 'first_name'):
                    val = get_first_name(tokens, recordDict)                    
                elif(column_name == 'middle_name'):
                    val = get_middle_name(tokens, recordDict)                    
                elif(column_name == 'deceased_indicator'):
                    person_org_indicator=recordDict['person_org_indicator']
                    val = get_deceased_indicator(person_org_indicator,tokens)
                elif(column_name == 'address_line_1'):
                    val = re_address_line_1(tokens[kw.extractColKey[column_name]])
                elif(column_name == 'address_line_2'):
                    val = re_address_line_2(tokens[kw.extractColKey[column_name]])
                elif column_name == 'prefix':
                    val = get_prefix_from_row_and_results(tokens, recordDict)
                elif column_name == 'province_state_code':
                    val = get_province_state_code_from_row(tokens)
                elif(column_name == 'postal_zip_code'):
                    pcode = tokens[kw.extractColKey[column_name]]
                    val = repostalCode(pcode)
                elif(column_name == 'phone_residential'):
                    phone = tokens[kw.extractColKey[column_name]]
                    val = rephone(phone)
                elif(column_name == 'phone_business'):
                    phone = tokens[kw.extractColKey[column_name]]
                    val = rephone_biz(phone)
                elif(column_name == 'phone_cell'):
                    phone = tokens[kw.extractColKey[column_name]]
                    val = rephone(phone)
                elif(column_name == 'phone_fax'):
                    phone = tokens[kw.extractColKey[column_name]]
                    val = rephone(phone)
                elif(column_name == 'e_mail_address_alternate'):
                    val = realtEmail(tokens[kw.extractColKey[column_name]].strip())
                elif column_name == 'personal_info_consent':
                    val = get_personal_info_consent()
                elif column_name == 'coseco_admin_system_type':
                    val = get_coseco_admin_system_type()
                elif column_name == 'e_mail_address_primary':
                    val = get_primary_email_from_row(tokens)
                elif(column_name == 'declined_e_mail'):
                    val = get_declined_email(tokens, recordDict)
                elif(column_name == 'country_code'): #other values?
                    val = recountryCode(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'gender'):
                    val = get_gender(tokens, recordDict)
                elif(column_name == 'occupation'):
                    val = get_occupation_code_from_row(tokens, recordDict)
                elif column_name == 'marital_status':
                    val = get_marital_status_from_source_and_results(tokens, recordDict)
                elif(column_name == 'language'):
                    val = get_language_from_row(tokens)
                elif(column_name == 'personal_info_consent'): #other values?
                    val = repersonalConsent(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'credit_score_consent_indicator'):
                    consent=tokens[kw.extractColKey[column_name]]
                    person_org_indicator=recordDict['person_org_indicator']
                    val = recredit_consent(tokens[kw.extractColKey[column_name]], person_org_indicator)
                elif(column_name == 'credit_score_consent_date'):
                    date=tokens[kw.extractColKey[column_name]]
                    person_org_indicator=recordDict['person_org_indicator']
                    val = recredit_date(date,person_org_indicator)
                elif(column_name == 'e_news_letters'): #other values?
                    val = reeletters(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'special_e_news_letters_for_business_owners'): #other values?
                    val = reeletters(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'e_campaigns'): #other values?
                    val = reeCampaigns(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'e_alerts_reminders'): #other values?
                    val = realerts(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'regular_mail'): #other values?
                    val = remail(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'business_partners_affiliates'): #other values?
                    val = rebusinessPartnersAffiliates(tokens[kw.extractColKey[column_name]].strip())
                elif(column_name == 'follow_up_date'):
                    val = get_follow_up_date_from_row(tokens)
                elif column_name == 'organization_type':
                    val = get_organization_type_from_destination_row(recordDict)
                elif column_name == 'care_of':
                    val = get_care_of_from_row(tokens)
                elif column_name == 'city':
                    val = translate_city(tokens)
                elif column_name == 'emergency_contact':
                    val = get_emergency_contact_from_row(tokens)
                elif column_name == 'best_time_to_contact':
                    val = get_best_time_to_call_from_row(tokens)
                else: ## blanket rule
                    val = reblanketRule(tokens[kw.extractColKey[column_name]].strip())
            else:
                val = ""
                missingColSet.add(column_name)

            recordDict[column_name] = val
        if numTokensUsed < kw.expectedColNum-tolerance or numTokensUsed >= (kw.expectedColNum + tolerance):
            #print("kw.expectedColNum=" + str(kw.expectedColNum) + ", numTokens=" + str(numTokensUsed))
            abnormalRecords+=1

        rec=""
        for a in kw.cifColNames:
            rec+=recordDict[a]
            rec+=","
        records.append(rec)

    return counter, records, missingColSet, skips

def main():
    args=sys.argv
    numArgs=len(args)

    print("Executing business rules...")
    if(numArgs<3):
    	print('number of arguments=' + str(numArgs))
    	print("need to provide files for extract, validation, load_filename, duplicate_filename and  in e.g. python coseco2cif.py extract.csv referenceload.csv autoload.csv ")
    	print('argument List:' + str(args))
    	cosecoExtract="data\\bsaExtract_v2.csv_mergedExtract.csv"
    	cifRefLoad="data\\bsaReferenceLoad_v2.csv"
    	print('using default files cosecoExtractFile:' + cosecoExtract + ' and validationRefFile:' + cifRefLoad)
    else:
    	cosecoExtract=args[1]
    	cifRefLoad=args[2]
    	
    with open(cosecoExtract, 'r') as content_file:
    	records = content_file.readlines()[1:]
    content_file.close()

    numExtractRecords = len(records)

    #timer start
    start = timeit.default_timer()

    counter, cifrecs, missingColSet, skips = businessRules(records)

    cifPartyLoad="autoload.csv"
    output = open(cifPartyLoad, 'w') 
    for c in kw.cifFormalName:
    	output.write(c + ",") 
    output.write("\n")
    

    for r in cifrecs:
    	output.write(r)
    	output.write("\n")
    output.close()

    #timer stop			
    stop = timeit.default_timer()
    execTime = stop-start

    print("\n\n== Conversion Summary ==")
    print("number of coseco extract (records, columns)=" + str(numExtractRecords))
    print("number of resultant cif records =" + str(counter))
    #print("abnormalRecords=" + str(abnormalRecords))
    print("records skipped =" + str(skips))
    print("missing columns in " + cosecoExtract + "=" + str(missingColSet))
    print("execution time(s)="+ str(round(execTime,3)) + "\n\n")


    print("Validating using AI merge...")

    import aicompare
    aicompare.aicompare(cifRefLoad, cifPartyLoad)

if __name__== "__main__":
	main()
