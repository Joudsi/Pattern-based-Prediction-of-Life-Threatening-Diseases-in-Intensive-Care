import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from IPython.display import display, HTML # used to print out pretty pandas dataframes
import matplotlib.dates as dates
import matplotlib.lines as mlines
from itertools import chain

# %matplotlib inline
plt.style.use('ggplot')


######################
# Database Connection
######################

# specify user/password/where the database is
sqluser = 'postgres'
sqlpass = 'postgres'
dbname = 'mimic'
schema_name = 'mimiciii'
host = 'localhost'

query_schema = 'SET search_path to ' + schema_name + ';'

# connect to the database
con = psycopg2.connect(dbname=dbname, user=sqluser, password=sqlpass, host=host)


########################
# initializing the lists
########################

l_subject_id = []
l_hadm_id = []
l_icu = []
l_amdission_age = []
l_gender = []
l_dod = []
l_admission_type = []
l_curr_service = []
l_thirty_day_mort = []

l_heartrate_mean = []
l_meanbp_mean = []
l_sysbp_mean = []
l_tempc_mean = []
l_spo2_mean = []
l_resprate_mean = []
l_diasbp_mean = []
l_glucose_mean = []

l_creatinine_max = []
l_potassium_max = []
l_sodium_max = []
l_chloride_max = []
l_bicarbonate_max = []
l_hematocrit_max = []
l_wbc_max = []
l_glucose_max = []
l_lactate_max = []
l_aniongap_max = []
l_albumin_max = []
l_bands_max = []
l_bilirubin_max = []
l_hemoglobin_max = []
l_ptt_max = []
l_inr_max = []
l_pt_max = []
l_bun_max = []



l_is_AKI = []
l_is_Betablocker = []


###########################################
#Retrieveing Subject_id for sepsis patients
###########################################

query = query_schema + """
select distinct subject_id
from angus_sepsis
where angus = 1
"""

df1 = pd.read_sql_query(query,con)

for i in range(0, len(df1['subject_id']) - 1):

    id = df1['subject_id'][i]

    #############################
    # Retrieveing Demographics
    #############################

    query = query_schema + """
    select subject_id, hadm_id, icustay_id, admission_age, gender, dod, admission_type, curr_service, thirty_day_mort
    from static_data
    where subject_id = """ + str(id)

    df2 = pd.read_sql_query(query, con)

    demographics = df2.values


    #############################
    # Retrieveing vital signs
    #############################

    query = query_schema + """
    select  heartrate_mean, meanbp_mean, sysbp_mean, tempc_mean, spo2_mean, resprate_mean, diasbp_mean, glucose_mean   
    from vitalsfirstday
    where subject_id = """ + str(id)

    df3 = pd.read_sql_query(query, con)

    vitals = df3.values



    #############################
    # Retrieveing Lab tests
    #############################

    query = query_schema + """
    select   creatinine_max, potassium_max, sodium_max, chloride_max, bicarbonate_max, hematocrit_max, wbc_max, glucose_max, lactate_max, aniongap_max, albumin_max, bands_max, bilirubin_max, hemoglobin_max, ptt_max, inr_max, pt_max, bun_max  
        from labsfirstday 
        where subject_id = """ + str(id)
    df4 = pd.read_sql_query(query, con)
    labs = df4.values

    #####################################
    # Retrieveing icd9_code to check AKI
    #####################################

    query = query_schema + """
            select icd9_code from diagnoses_icd where subject_id = """ + str(id)

    df5 = pd.read_sql_query(query, con)

    icd9_code = df5.values


    #####################################
    # Retrieveing betablocker to check AKI
    #####################################

    query = query_schema + """                                                   
            select drug_name_generic from prescriptions where subject_id = """ + str(id)

    df6 = pd.read_sql_query(query, con)

    betablocker = df6.values

    print(betablocker)


    ###########################
    # filling lists with values
    ###########################

    if (df2.size == 0 or df3.size == 0 or df4.size == 0) :
        continue

    # filling the demographics lists

    l_subject_id.append(demographics[0][0])
    l_hadm_id.append(demographics[0][1])
    l_icu.append(demographics[0][2])
    l_amdission_age.append(demographics[0][3])
    l_gender.append(1) if (demographics[0][4] == 'F') else  l_gender.append(0)
    # l_gender.append(demographics[0][4])
    l_dod.append(demographics[0][5])
    l_admission_type.append(demographics[0][6])
    l_curr_service.append(demographics[0][7])
    l_thirty_day_mort.append(1) if (demographics[0][8] == 'Y') else  l_thirty_day_mort.append(0)
    # l_thirty_day_mort.append(demographics[0][8])

    # filling the vitals lists

    l_heartrate_mean.append(vitals[0][0])
    l_meanbp_mean.append(vitals[0][1])
    l_sysbp_mean.append(vitals[0][2])
    l_tempc_mean.append(vitals[0][3])
    l_spo2_mean.append(vitals[0][4])
    l_resprate_mean.append(vitals[0][5])
    l_diasbp_mean.append(vitals[0][6])
    l_glucose_mean.append(vitals[0][7])

    # filling the labs lists

    l_creatinine_max.append(labs[0][0])
    l_potassium_max.append(labs[0][1])
    l_sodium_max.append(labs[0][2])
    l_chloride_max.append(labs[0][3])
    l_bicarbonate_max.append(labs[0][4])
    l_hematocrit_max.append(labs[0][5])
    l_wbc_max.append(labs[0][6])
    l_glucose_max.append(labs[0][7])
    l_lactate_max.append(labs[0][8])
    l_aniongap_max.append(labs[0][9])
    l_albumin_max.append(labs[0][10])
    l_bands_max.append(labs[0][11])
    l_bilirubin_max.append(labs[0][12])
    l_hemoglobin_max.append(labs[0][13])
    l_ptt_max.append(labs[0][14])
    l_inr_max.append(labs[0][15])
    l_pt_max.append(labs[0][16])
    l_bun_max.append(labs[0][17])

    # filling the icd9_code lists

    l_is_AKI.append(1) if '5849' in icd9_code else l_is_AKI.append(0)


    # filling the betablocker list

    bb = ['Acebutolol','Atenolol','Metoprolol','Nadolol','Nebivolol' ,'Propranolol','Betaxolol' ,'Bisoprolol','Carteolol' ,'Carvedilol','Labetalol' ,'Nebivolol' ,'Penbutolol','Pindolol'  ,'Sotalol'   ,'Timolol' ] 

    if bb  in betablocker:
        l_is_Betablocker.append(1)
    else:
        l_is_Betablocker.append(0)


#######################################################

print(len(l_subject_id))
print(len(l_hadm_id))
print(len(l_icu))
print(len(l_amdission_age))
print(len(l_gender))
print(len(l_dod))
print(len(l_admission_type))
print(len(l_curr_service))
print(len(l_thirty_day_mort))
print(len(l_heartrate_mean))
print(len(l_meanbp_mean))
print(len(l_sysbp_mean))
print(len(l_tempc_mean))
print(len(l_spo2_mean))
print(len(l_resprate_mean))
print(len(l_creatinine_max))
print(len(l_potassium_max))
print(len(l_sodium_max))
print(len(l_chloride_max))
print(len(l_bicarbonate_max))
print(len(l_hematocrit_max))
print(len(l_wbc_max))
print(len(l_glucose_max))
print(len(l_lactate_max))#
print(len(l_aniongap_max))
print(len(l_albumin_max))
print(len(l_bands_max))
print(len(l_bilirubin_max))
print(len(l_hemoglobin_max))
print(len(l_lactate_max))       #
print(len(l_ptt_max))
print(len(l_inr_max))
print(len(l_pt_max))
print(len(l_bun_max))
print(len(l_is_AKI))
print(len(l_is_Betablocker))

data = pd.DataFrame({ 'subject_id' : l_subject_id,
'hadm_id' : l_hadm_id,
'icu' : l_icu,
'amdission_age' : l_amdission_age,
'gender' : l_gender,
# 'dod' : l_dod,
# 'admission_type' : l_admission_type,
# 'curr_service' : l_curr_service,
'thirty_day_mort' : l_thirty_day_mort,
'heartrate_mean' : l_heartrate_mean,
'meanbp_mean' : l_meanbp_mean,
'sysbp_mean' : l_sysbp_mean,
'tempc_mean' : l_tempc_mean,
'spo2_mean' : l_spo2_mean,
'resprate_mean' : l_resprate_mean,
'diasbp_mean' : l_diasbp_mean,
'glucose_vitals' : l_glucose_mean,
'creatinine_max' : l_creatinine_max,
'potassium_max' : l_potassium_max,
'sodium_max' : l_sodium_max,
'chloride_max' : l_chloride_max,
'bicarbonate_max' : l_bicarbonate_max,
'hematocrit_max' : l_hematocrit_max,
'wbc_max' : l_wbc_max,
'glucose_max' : l_glucose_max,
'lactate_max' : l_lactate_max,
'aniongap_max' : l_aniongap_max,
'albumin_max' : l_albumin_max,
'bands_max' : l_bands_max,
'bilirubin_max' : l_bilirubin_max,
'hemoglobin_max' : l_hemoglobin_max,
'ptt_max' : l_ptt_max,
'inr_max' : l_inr_max,
'pt_max' : l_pt_max,
'bun_max' : l_bun_max,
'is_AKI' : l_is_AKI,
'is_Betablocker' : l_is_Betablocker
  }, columns=['subject_id','hadm_id', 'icu', 'amdission_age', 'gender', 'thirty_day_mort', 'heartrate_mean', 'meanbp_mean', 'sysbp_mean','tempc_mean','spo2_mean','resprate_mean', 'glucose_vitals' ,'creatinine_max','potassium_max','sodium_max','chloride_max' ,'bicarbonate_max','hematocrit_max','wbc_max','glucose_max','lactate_max', 'aniongap_max','albumin_max','bands_max','bilirubin_max','hemoglobin_max','lactate_max','ptt_max','inr_max','pt_max','bun_max', 'is_AKI', 'is_Betablocker'])


pd.set_option('display.max_rows', 500)
print(data.describe())
data.to_excel("/Users/joudsi/Desktop/output.xlsx")
data.to_csv("/Users/joudsi/Desktop/output.csv")