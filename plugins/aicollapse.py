#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import jellyfish
from fuzzywuzzy import fuzz
import timeit
import sys
import metadata as kw

DevColumns=['Source System Client No.','Grouping ID','Loyalty Date','Client Name','First Name','Last Name','Address Line 1','CITY','Phone - Residential','Policy Role']

def is_blank(val):
    return not (val and str(val).strip())

def removeLeadingTrailingSpaces(raw_df):
    # strip leading/trailing whitespaces in column values
    rawdata_df = raw_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # strip leading/trailing whitespaces in column names
    rawdata_df.rename(columns=lambda x: x.strip()) 
    return rawdata_df

# Rule 1
def groupby(rawdata_df, grpby_attr):
    return rawdata_df.groupby(grpby_attr)  

# Rule 2
def one_but_blank(hhmem_rec, non_hhmem_rec, cellkey):
    hhmem_val=hhmem_rec[cellkey]
    non_hhmem_val=non_hhmem_rec[cellkey]

    if(is_blank(hhmem_val) and not is_blank(non_hhmem_val)):
        return non_hhmem_val

# Rule 3: When the values are the same in both compared records, randomly select one for the collapsed record (applies to all fields)
def both_same(hhmem_rec, non_hhmem_rec, cellkey):
    hhmem_val=hhmem_rec[cellkey]
    non_hhmem_val=non_hhmem_rec[cellkey]

    if(not is_blank(hhmem_val) and not is_blank(non_hhmem_val)):
        return (str(hhmem_val).strip().upper()==str(non_hhmem_val).strip().upper())

# Rule 4: When the values are the same in both compared records, randomly select one for the collapsed record (applies to all fields)
def collapse_loyalty_date(hhmem_rec, non_hhmem_rec):
    cellkey='Loyalty Date'

    hhmem_val=str(hhmem_rec[cellkey])
    non_hhmem_val=str(non_hhmem_rec[cellkey])

    return min(hhmem_val, non_hhmem_val)

## Rule 7, 8
def collapse_phone(hhmem_rec, non_hhmem_rec, cellkey): 
    hhmem_val=str(hhmem_rec[cellkey])
    non_hhmem_val=str(non_hhmem_rec[cellkey])
    ph_dict={}
    ph_dict[hhmem_val]=len(hhmem_val)
    ph_dict[non_hhmem_val]=len(non_hhmem_val)                                                
                                                    
    if(ph_dict[non_hhmem_val]<10):
        pass # do nothing
    elif(ph_dict[hhmem_val]<10):
        return non_hhmem_val
    else: # both ph numbers are > 10
        return hhmem_val

## Rule 6:
def same_client_name(hhmem_rec, non_hhmem_rec, fuzzy_threshold):
    cellkey='Client Name'

    hhmem_val=str(hhmem_rec[cellkey])
    non_hhmem_val=str(non_hhmem_rec[cellkey])

    fuzzy_ratio = fuzz.ratio(hhmem_val, non_hhmem_val)
    return (fuzzy_ratio > fuzzy_threshold)

## Rule 5:
def chose_prio_rec_as_base(grp):
    order_of_preference = ['HouseholdMember', 'PropertyOwner','RegisteredOwner','ThirdPartyPayor','Driver']            
    reference=''

    pref_role=""
    for preference in order_of_preference:
        pref_role = grp.loc[grp['Policy Role'] == preference]
        if(len(pref_role.index)!=0): # non-emtpy dataframe            
            reference=preference
            break

    base_rec_dict = pref_role.iloc[0].to_dict()

    conflict = False # conflict
    if(len(pref_role.index) > 1 ):
        ref_rec_df = grp.loc[grp['Policy Role'] == reference]
        for index, row in ref_rec_df.iterrows():
            if(index==0):
                first_rec_dict = row.to_dict()
                if(base_rec_dict != first_rec_dict): # sanity check -- they should be equal                    
                    print("************************ first and base dict not equal ************************")                    
            else:
                rec_dict = row.to_dict()
                if(base_rec_dict == rec_dict): #
                    continue
                else: # iterate through the records
                    for key, base_val in base_rec_dict.items():
                        rec_val = rec_dict[key]

                        # if rec_val is blank, do nothing
                        if(is_blank(rec_val)):
                            continue

                        # copy over the non-blank value to base_val 
                        if(is_blank(base_val) and not is_blank(rec_val)):
                            base_rec_dict[key] = rec_val
                            continue

                        # if both base_val and rec_val are not blank
                        if(not (is_blank(base_val) or is_blank(rec_val))):
                            base_val_prime=str(base_val).strip().upper()
                            rec_val_prime=str(rec_val).strip().upper()
                            if(base_val_prime == rec_val_prime):
                                continue
                            else:
                                conflict = True # conflict

    return reference, base_rec_dict, conflict

## Rule 13: If different genders found, then derive the gender from prefix as the final collapsed value. 
def collapse_gender(ref_rec, non_ref_rec):
    mainkey='Preferred Language'
    helperkey='Prefix'
    permitted_values = ['1','2']

    ref_val=str(ref_rec[mainkey]).strip().upper()
    non_ref_val=str(non_ref_rec[mainkey]).strip().upper()

    gender=''
    if(ref_val in permitted_values):
        gender=ref_val # default is ref based val

    if(ref_val in permitted_values and
        non_ref_val not in permitted_values):
        return ref_val

    if(non_ref_val in permitted_values and
        ref_val not in permitted_values):
        return non_ref_val

    # both ref_val and non_ref_val are legit
    if(ref_val in permitted_values and (ref_val==non_ref_val)):
        return ref_val   
    
    # seek help from prefix
    ref_prefix_val=str(ref_rec[helperkey]).strip().upper()
    if(ref_prefix_val in kw.extrac_prefix_2_gender_mapping.keys()):
        gender=kw.extrac_prefix_2_gender_mapping[ref_prefix_val]

    return gender


## Rule 14: If both 'E' and 'F' found, then choose 'F' as the final collapsed value for [preferred language].
def collapse_preferred_lang(ref_rec, non_ref_rec):
    cellkey='Preferred Language'

    ref_val=str(ref_rec[cellkey]).strip().upper()
    non_ref_val=str(non_ref_rec[cellkey]).strip().upper()
    
    if(ref_val=='F' or non_ref_val=='F'):
        return 'F'

    if(ref_val=='E' or non_ref_val=='E'):
        return 'E'
    
    return ''

## Rule 15: Allowable values for Personal Info Consents are 0/1/2/3. If various values available, then pick the smallest value. 
def collapse_personal_info_consent(ref_rec, non_ref_rec):
    cellkey='Personal Info Consent'

    ref_val=str(ref_rec[cellkey]).strip().upper()
    non_ref_val=str(non_ref_rec[cellkey]).strip().upper()
    
    permitted_values = ['0', '1', '2', '3']

    if(ref_val in permitted_values and 
            non_ref_val in permitted_values): 
        return min(ref_val, non_ref_val)

    if(ref_val not in permitted_values):
       ref_val='999'

    if(non_ref_val not in permitted_values):
       non_ref_val='999'

    min_val=min(ref_val, non_ref_val)
    if(min_val=='999'):
        min_val=''

    return min_val

# Rule 16, 17: [Credit Score Consent Indicator] allowable values are : 'Y'/'N'
# If different values are found, then pick the final collapsed value based on more recent [Credit Score Consent Date], 
# set the  [Credit Score Consent Date] as such. 
def collapse_credit_score_consent_date_n_indicator(ref_rec, non_ref_rec):
    mainkey='Credit Score Consent Indicator'
    helperkey='Credit Score Consent Date'

    ref_date = str(ref_rec[helperkey])
    non_ref_date = str(non_ref_rec[helperkey])

    pref = consent_date = ''
    if(ref_date >= non_ref_date):
        consent_date=ref_date
        pref=ref_rec
    else:
        consent_date=non_ref_date
        pref=non_ref_rec

    permitted_values = ['Y','N']

    ref_val=str(ref_rec[mainkey]).strip().upper()
    non_ref_val=str(non_ref_rec[mainkey]).strip().upper()

    indicator=''
    if(ref_val in permitted_values):
        indicator=ref_val # default is ref based val

    if(ref_val in permitted_values and
        non_ref_val not in permitted_values):
        indicator=ref_val

    if(non_ref_val in permitted_values and
        ref_val not in permitted_values):
        indicator=non_ref_val

    # both ref_val and non_ref_val are legit, and equal
    if(ref_val in permitted_values and (ref_val==non_ref_val)):
        indicator=ref_val   

    # both ref_val and non_ref_val are legit, but conflict    
    # seek help from most recent consent date rec
    indicator_val=str(pref[mainkey]).strip().upper()
    if(indicator_val in permitted_values):
        indicator=indicator_val

    return indicator, consent_date

# Rule 21: Consent Indicators
def collapse_consent_indicators_n_NDNCL_date(ref_rec, non_ref_rec, cellkey):
    mainkey=cellkey
    helperkey='Consent-NDNCL Date'

    ref_date = str(ref_rec[helperkey])
    non_ref_date = str(non_ref_rec[helperkey])

    pref = date = ''
    if(ref_date >= non_ref_date):
        date=ref_date
        pref=ref_rec
    else:
        date=non_ref_date
        pref=non_ref_rec

    permitted_values = ['Y','N']

## Rule 13: If different genders found, then derive the gender from prefix as the final collapsed value. 
def collapse_gender(ref_rec, non_ref_rec):
    mainkey='Preferred Language'
    helperkey='Prefix'
    permitted_values = ['1','2']

    ref_val=str(ref_rec[mainkey]).strip().upper()
    non_ref_val=str(non_ref_rec[mainkey]).strip().upper()

    gender=''
    if(ref_val in permitted_values):
        gender=ref_val # default is ref based val

    if(ref_val in permitted_values and
        non_ref_val not in permitted_values):
        return ref_val

    if(non_ref_val in permitted_values and
        ref_val not in permitted_values):
        return non_ref_val

    # both ref_val and non_ref_val are legit
    if(ref_val in permitted_values and (ref_val==non_ref_val)):
        return ref_val   
    
    # seek help from prefix
    ref_prefix_val=str(ref_rec[helperkey]).strip().upper()
    if(ref_prefix_val in kw.extract_prefix_2_gender_mapping.keys()):
        gender=kw.extract_prefix_2_gender_mapping[ref_prefix_val]

    return gender


## Rule 14: If both 'E' and 'F' found, then choose 'F' as the final collapsed value for [preferred language].
def collapse_preferred_lang(ref_rec, non_ref_rec):
    cellkey='Preferred Language'

    ref_val=str(ref_rec[cellkey]).strip().upper()
    non_ref_val=str(non_ref_rec[cellkey]).strip().upper()
    
    if(ref_val=='F' or non_ref_val=='F'):
        return 'F'

    if(ref_val=='E' or non_ref_val=='E'):
        return 'E'
    
    return ''

## Rule 15: Allowable values for Personal Info Consents are 0/1/2/3. If various values available, then pick the smallest value. 
def collapse_personal_info_consent(ref_rec, non_ref_rec):
    cellkey='Personal Info Consent'

    ref_val=str(ref_rec[cellkey]).strip().upper()
    non_ref_val=str(non_ref_rec[cellkey]).strip().upper()
    
    permitted_values = ['0', '1', '2', '3']

    if(ref_val in permitted_values and 
            non_ref_val in permitted_values): 
        return min(ref_val, non_ref_val)

    if(ref_val not in permitted_values):
       ref_val='999'

    if(non_ref_val not in permitted_values):
       non_ref_val='999'

    min_val=min(ref_val, non_ref_val)
    if(min_val=='999'):
        min_val=''

    return min_val

# Rule 16, 17: [Credit Score Consent Indicator] allowable values are : 'Y'/'N'
# If different values are found, then pick the final collapsed value based on more recent [Credit Score Consent Date], 
# set the  [Credit Score Consent Date] as such. 
def collapse_credit_score_consent_date_n_indicator(ref_rec, non_ref_rec):
    mainkey='Credit Score Consent Indicator'
    helperkey='Credit Score Consent Date'

    ref_date = str(ref_rec[helperkey])
    non_ref_date = str(non_ref_rec[helperkey])

    pref = consent_date = ''
    if(ref_date >= non_ref_date):
        consent_date=ref_date
        pref=ref_rec
    else:
        consent_date=non_ref_date
        pref=non_ref_rec

    permitted_values = ['Y','N']

    ref_val=str(ref_rec[mainkey]).strip().upper()
    non_ref_val=str(non_ref_rec[mainkey]).strip().upper()

    indicator=''
    if(ref_val in permitted_values):
        indicator=ref_val # default is ref based val

    if(ref_val in permitted_values and
        non_ref_val not in permitted_values):
        indicator=ref_val

    if(non_ref_val in permitted_values and
        ref_val not in permitted_values):
        indicator=non_ref_val

    # both ref_val and non_ref_val are legit, and equal
    if(ref_val in permitted_values and (ref_val==non_ref_val)):
        indicator=ref_val   

    # both ref_val and non_ref_val are legit, but conflict    
    # seek help from most recent consent date rec
    indicator_val=str(pref[mainkey]).strip().upper()
    if(indicator_val in permitted_values):
        indicator=indicator_val

    return indicator, consent_date

def inconsistent_rec_type(ref_rec, non_ref_rec, cellkey):
    ref = str(ref_rec[cellkey]).strip().upper()
    non_ref = str(non_ref_rec[cellkey]).strip().upper()

    return (ref != non_ref)

# Rule 21: Consent Indicators
def collapse_consent_indicators_n_NDNCL_date(ref_rec, non_ref_rec, cellkey):
    mainkey=cellkey
    helperkey='Consent-NDNCL Date'

    ref_date = str(ref_rec[helperkey])
    non_ref_date = str(non_ref_rec[helperkey])

    pref = date = ''
    if(ref_date >= non_ref_date):
        date=ref_date
        pref=ref_rec
    else:
        date=non_ref_date
        pref=non_ref_rec

    permitted_values = ['Y','N']

    ref_val=str(ref_rec[mainkey]).strip().upper()
    non_ref_val=str(non_ref_rec[mainkey]).strip().upper()

    indicator=''
    if(ref_val in permitted_values):
        indicator=ref_val # default is ref based val

    if(ref_val in permitted_values and
        non_ref_val not in permitted_values):
        indicator=ref_val

    if(non_ref_val in permitted_values and
        ref_val not in permitted_values):
        indicator=non_ref_val

    # both ref_val and non_ref_val are legit, and equal
    if(ref_val in permitted_values and (ref_val==non_ref_val)):
        indicator=ref_val   

    # both ref_val and non_ref_val are legit, but conflict    
    # seek help from most recent date rec
    indicator_val=str(pref[mainkey]).strip().upper()
    if(indicator_val in permitted_values):
        indicator=indicator_val

    return indicator, date

def addRecToWarnOrErr(msg, rec_dict):
    ref_list = list(rec_dict.values())
    ref_list.append(msg)
    return ref_list

def aiCollapseWtRules(raw_df, grpby_attr, fuzzy_threshold):
    cols=kw.extractFormalNames
    rawdata_df = removeLeadingTrailingSpaces(raw_df)
    grpby_df = groupby(rawdata_df, grpby_attr)
    res_df=pd.DataFrame(columns=cols)
    collapsed_matrix=[]
    error_matrix=[]
    warn_matrix=[]

    for clientno, grp in grpby_df:
        conflict = False
        
        errorRecFound = False
        err_ref_recorded = False
        err_msg = ""        
        error_rec_key = ""

        warnRecFound = False
        warn_ref_recorded = False
        warn_msg = ""

        if(len(grp.index) == 1 ): # only one record in the group, no need for collapsing
            for row, data in grp.iterrows():
                hhmem = data
                hhmem_dict=hhmem.T.to_dict()
                collapsed_matrix.append(hhmem_dict.values())        
        else:            
            ## Rule 5
            ## base_dict is the preference or the reference record, and becomes the default record. we do changes to base_dict
            ## we check for the conflict group wide
            reference, base_dict, conflict = chose_prio_rec_as_base(grp) 

            if(conflict):
                err_msg += "Different 'Policy Role' i.e. " + reference + " values detected, and no decision can be made; "
                for index, row in grp.iterrows():
                    val_list = addRecToWarnOrErr(err_msg, row.to_dict())
                    error_matrix.append(val_list)
                continue # no need to continue with collapsing this group as conflict detected, continue to next group            

            if not list(base_dict.keys()): # no key available, bad record. TODO: need to record 
                continue

            non_base_df = grp.loc[grp['Policy Role'] != reference]
            for index, row in non_base_df.iterrows():
                rec_dict = row.to_dict()                   
                    
                for cellkey in cols:
                    rec_val=rec_dict[cellkey]
                    base_val=base_dict[cellkey]
                  
                    #### BLANKET RULES #### (order matters)                   

                    # if rec_val is blank, do nothing
                    if(is_blank(rec_val)):
                        continue

                    # copy over the non-blank value to base val                    
                    # Rule 2
                    val = one_but_blank(base_dict, rec_dict, cellkey)
                    if(val):
                        base_dict[cellkey]=val
                        continue
                                                                        
                    # if hhmem val and non_hhmem val are equal, do nothing
                    # Rule 3
                    val = both_same(base_dict, rec_dict, cellkey)
                    if(val):
                        # do nothing
                        continue
                        
                    #### CUSTOM RULES #### 
                    # if hhmem val and non_hhmem val are NOT blank, apply CUSTOM RULES
                    if(not (is_blank(base_val) or is_blank(rec_val))):

                        # Rule 4
                        if(cellkey=='Loyalty Date'):
                            base_dict[cellkey] = collapse_loyalty_date(base_dict, rec_dict)
                            continue
                        
                        # Rule 7,8    
                        if(cellkey in ['Phone - Residential', 'Phone - Business', 'Phone - Fax', 'Phone - Cell', 'Phone - Alternate']):
                            val = collapse_phone(base_dict, rec_dict, cellkey)
                            if(val):
                                base_dict[cellkey]=val                                                                                                
                            continue

                        # Rule 13
                        if(cellkey=='Gender'):
                            val = collapse_gender(base_dict, rec_dict)
                            base_dict[cellkey]=val                                                                                            
                            continue

                        # Rule 14
                        if(cellkey=='Preferred Language'):
                            val = collapse_preferred_lang(base_dict, rec_dict)
                            if(val):
                                base_dict[cellkey]=val                                                                                                
                            continue

                        # Rule 15
                        if(cellkey=='Personal Info Consent'):
                            val = collapse_personal_info_consent(base_dict, rec_dict)
                            if(val):
                                base_dict[cellkey]=val                                                                                                
                            continue

                        # Rule 16, 17                        
                        if(cellkey=='Credit Score Consent Indicator'):
                            indicator,date = collapse_credit_score_consent_date_n_indicator(base_dict, rec_dict)
                            base_dict[cellkey]=indicator
                            base_dict['Credit Score Consent Date']=date                                                                                                
                            continue

                        # Rule 21
                        if(cellkey in ['Consent-NDNCL Date', 'Consent to Call', 'Consent-Line of Business', 'Consent-Phone Number', 'Consent-Household Member']):
                            indicator,date = collapse_consent_indicators_n_NDNCL_date(base_dict, rec_dict, cellkey)
                            base_dict[cellkey]=indicator
                            base_dict['Consent-NDNCL Date']=date                                                                                                
                            continue

                        if(cellkey == 'RecordType'):
                            if(inconsistent_rec_type(base_dict, rec_dict, cellkey)):
                                errorRecFound=True
                                err_msg += "inconsistent_rec_type found i.e. mix of NEW, CHG, URGENT; "
                            continue

                        if(cellkey in ['Group Name', 'Office No']):
                            if(inconsistent_rec_type(base_dict, rec_dict, cellkey)):
                                warnRecFound=True 
                                warn_msg += "More than one 'Group Name' or 'Office No' associated with this client; "
                            continue
                            
                        # Rule 6
                        if(cellkey=='Client Name'):                          
                            hhmem_err_recr=""
                            non_hhmem_err_recr=""
                            if(not same_client_name(base_dict, rec_dict, fuzzy_threshold)):
                                errorRecFound=True 
                                err_msg += "inconsistent client name; "
                            continue

            if(errorRecFound):
                for index, row in grp.iterrows(): # if err is found with any contituent records, then push all to log
                    val_list = addRecToWarnOrErr(err_msg, row.to_dict())
                    error_matrix.append(val_list)


            if(warnRecFound):
                for index, row in grp.iterrows(): # if err is found with any contituent records, then push all to log
                    val_list = addRecToWarnOrErr(warn_msg, row.to_dict())
                    warn_matrix.append(val_list)

            if(not errorRecFound):               
                collapsed_matrix.append(base_dict.values())

    res_df = pd.DataFrame(collapsed_matrix, columns=cols)
    err_df = pd.DataFrame(error_matrix, columns=cols + ['Error'])
    warn_df = pd.DataFrame(warn_matrix, columns=cols + ['Warn'])

    return len(rawdata_df.index), res_df, err_df, warn_df



def main():
    print("Executing using AI collapse...")
    fuzzyThreshold=82
    args=sys.argv
    numArgs=len(args)

    if(numArgs<3):
        print('number of arguments=' + str(numArgs))
        print('need to provide files to consume and compare e.g. extract.csv validationRefFile.csv')
        print('argument List:' + str(args))
        cosecoExtractFile="test\\input_files\\raw_extract_4_collapse_v2.csv"
        validationRefFile="test\\input_files\\collapsed_extract_v2_validation.csv"
        print('using default files cosecoExtractFile:' + cosecoExtractFile + ' and validationRefFile:' + validationRefFile)
        file1=cosecoExtractFile
        file2=validationRefFile
    else:
        file1=args[1]
        cosecoExtractFile=file1
        file2=args[2]
        validationRefFile=file2
    
    print("file1=" + file1)
    print("file2=" + file2)

    #timer start
    start = timeit.default_timer()

    records_df = pd.read_csv(file1, keep_default_na=False, skip_blank_lines=True)   
    groupBy='Source System Client No.'
    numExtractRecords, collapsed_df, err_recs_df, warn_df=aiCollapseWtRules(records_df,groupBy,fuzzyThreshold)    

    mergedFile=cosecoExtractFile + "_collapsedExtract_auto.csv"
    collapsed_df.to_csv(mergedFile,index=False)

    if(len(err_recs_df.index) > 0):
        errFile=cosecoExtractFile + "_collapsing_err_auto.csv"
        err_recs_df.to_csv(errFile,index=False)
        
    stop = timeit.default_timer()
    execTime = stop-start
    print("execution time(s)="+ str(round(execTime,3)) + "\n\n")

    print("Validating using AI merge...")

    import aicompare
    aicompare.aicompare(validationRefFile, mergedFile)
  
if __name__== "__main__":
    main()
