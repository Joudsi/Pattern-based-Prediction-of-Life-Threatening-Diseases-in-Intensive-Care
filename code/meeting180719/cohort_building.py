import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from IPython.display import display, HTML  # used to print out pretty pandas dataframes
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
l_glucose_vital_mean = []

l_creatinine_mean = []
l_potassium_mean = []
l_sodium_mean = []
l_chloride_mean = []
l_bicarbonate_mean = []
l_hematocrit_mean = []
l_wbc_mean = []
l_glucose_mean = []
l_lactate_mean = []
l_aniongap_mean = []
l_albumin_mean = []
l_bands_mean = []
l_bilirubin_mean = []
l_hemoglobin_mean = []
l_ptt_mean = []
l_inr_mean = []
l_pt_mean = []
l_bun_mean = []

l_is_AKI = []
l_is_Betablocker = []

###########################################
# Retrieveing Subject_id for sepsis patients
###########################################

query = query_schema + """
select distinct subject_id
from angus_sepsis
where angus = 1
"""

df1 = pd.read_sql_query(query, con)

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
    select  heartrate_mean,  meanbp_mean, sysbp_mean,  tempc_mean,  spo2_mean, resprate_mean,  diasbp_mean, glucose_mean   
    from vitalsfirstday
    where subject_id = """ + str(id)

    df3 = pd.read_sql_query(query, con)

    vitals = df3.values

    #############################
    # Retrieveing Lab tests
    #############################

    query = query_schema + """
    select   creatinine_mean, potassium_mean, sodium_mean, chloride_mean,
     bicarbonate_mean, hematocrit_mean, wbc_mean, glucose_mean,
       lactate_mean, aniongap_mean, albumin_mean, bands_mean, 
       bilirubin_mean, hemoglobin_mean, ptt_mean, inr_mean,  pt_mean,  bun_mean
        from labsfirstday1 
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

    if (df2.size == 0 or df3.size == 0 or df4.size == 0):
        continue

    # filling the demographics lists

    l_subject_id.append(demographics[0][0])
    l_hadm_id.append(demographics[0][1])
    l_icu.append(demographics[0][2])
    l_amdission_age.append(demographics[0][3])
    l_gender.append(1) if (demographics[0][4] == 'F') else l_gender.append(0)
    # l_gender.append(demographics[0][4])
    l_dod.append(demographics[0][5])
    l_admission_type.append(demographics[0][6])
    l_curr_service.append(demographics[0][7])
    l_thirty_day_mort.append(1) if (demographics[0][8] == 'Y') else l_thirty_day_mort.append(0)
    # l_thirty_day_mort.append(demographics[0][8])

    # filling the vitals lists

    l_heartrate_mean.append(vitals[0][0])

    l_meanbp_mean.append(vitals[0][1])

    l_sysbp_mean.append(vitals[0][2])

    l_tempc_mean.append(vitals[0][3])

    l_spo2_mean.append(vitals[0][4])

    l_resprate_mean.append(vitals[0][5])

    l_diasbp_mean.append(vitals[0][6])

    l_glucose_vital_mean.append(vitals[0][7])

    # filling the labs lists

    l_creatinine_mean.append(labs[0][0])
    l_potassium_mean.append(labs[0][1])
    l_sodium_mean.append(labs[0][2])
    l_chloride_mean.append(labs[0][3])
    l_bicarbonate_mean.append(labs[0][4])
    l_hematocrit_mean.append(labs[0][5])
    l_wbc_mean.append(labs[0][6])
    l_glucose_mean.append(labs[0][7])
    l_lactate_mean.append(labs[0][8])
    l_aniongap_mean.append(labs[0][9])
    l_albumin_mean.append(labs[0][10])
    l_bands_mean.append(labs[0][11])
    l_bilirubin_mean.append(labs[0][12])
    l_hemoglobin_mean.append(labs[0][13])
    l_ptt_mean.append(labs[0][14])
    l_inr_mean.append(labs[0][15])
    l_pt_mean.append(labs[0][16])
    l_bun_mean.append(labs[0][17])

    # filling the icd9_code lists

    l_is_AKI.append(1) if '5849' in icd9_code else l_is_AKI.append(0)

    # filling the betablocker list

    bb = ['Acebutolol', 'Atenolol', 'Metoprolol', 'Nadolol', 'Nebivolol', 'Propranolol', 'Betaxolol', 'Bisoprolol',
          'Carteolol', 'Carvedilol', 'Labetalol', 'Nebivolol', 'Penbutolol', 'Pindolol', 'Sotalol', 'Timolol']

    if bb in betablocker:
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


print(len(l_creatinine_mean))
print(len(l_potassium_mean))
print(len(l_sodium_mean))
print(len(l_chloride_mean))
print(len(l_bicarbonate_mean))
print(len(l_hematocrit_mean))
print(len(l_wbc_mean))
print(len(l_glucose_mean))
print(len(l_lactate_mean))#
print(len(l_aniongap_mean))
print(len(l_albumin_mean))
print(len(l_bands_mean))
print(len(l_bilirubin_mean))
print(len(l_hemoglobin_mean))
print(len(l_lactate_mean))       #
print(len(l_ptt_mean))
print(len(l_inr_mean))
print(len(l_pt_mean))
print(len(l_bun_mean))

print(len(l_is_AKI))
print(len(l_is_Betablocker))

data = pd.DataFrame({'subject_id': l_subject_id,
                     'hadm_id': l_hadm_id,
                     'icu': l_icu,
                     'amdission_age': l_amdission_age,
                     'gender': l_gender,
                     # 'dod': l_dod,
                     # 'admission_type': l_admission_type,
                     # 'curr_service': l_curr_service,
                     'thirty_day_mort': l_thirty_day_mort,
                     'heartrate_mean': l_heartrate_mean,
                     'meanbp_mean': l_meanbp_mean,
                     'sysbp_mean': l_sysbp_mean,
                     'tempc_mean': l_tempc_mean,
                     'spo2_mean': l_spo2_mean,
                     'resprate_mean': l_resprate_mean,
                     'diasbp_mean': l_diasbp_mean,
                     # 'glucose_mean_vitals': l_glucose_vital_mean,
                     'creatinine_mean': l_creatinine_mean,
                     'potassium_mean': l_potassium_mean,
                     'sodium_mean': l_sodium_mean,
                     'chloride_mean': l_chloride_mean,
                     'bicarbonate_mean': l_bicarbonate_mean,
                     'hematocrit_mean': l_hematocrit_mean,
                     'wbc_mean': l_wbc_mean,
                     'glucose_mean': l_glucose_mean,
                     'lactate_mean': l_lactate_mean,
                     'aniongap_mean': l_aniongap_mean,
                     'albumin_mean': l_albumin_mean,
                     'bands_mean': l_bands_mean,
                     'bilirubin_mean': l_bilirubin_mean,
                     'hemoglobin_mean': l_hemoglobin_mean,
                     'ptt_mean': l_ptt_mean,
                     'inr_mean': l_inr_mean,
                     'pt_mean': l_pt_mean,
                     'bun_mean': l_bun_mean,
                     'is_AKI': l_is_AKI,
                     'is_Betablocker': l_is_Betablocker
                     }, columns=['subject_id', 'hadm_id', 'icu', 'amdission_age', 'gender',
                                 # 'dod', 'admission_type', 'curr_service',
                                 'thirty_day_mort',
                                'heartrate_mean',
                                 'meanbp_mean', 'sysbp_mean',
                                 'tempc_mean', 'spo2_mean',
                                 'resprate_mean',
                                 'diasbp_mean',
                                 # 'glucose_mean_vitals',
                                 'creatinine_mean', 'potassium_mean',
                                 'sodium_mean', 'chloride_mean',
                                 'bicarbonate_mean', 'hematocrit_mean',
                                 'wbc_mean',
                                 'glucose_mean', 'lactate_mean', 'aniongap_mean',
                                 'albumin_mean', 'bands_mean',
                                 'bilirubin_mean', 'hemoglobin_mean',
                                 'ptt_mean', 'inr_mean', 'pt_mean',
                                'bun_mean', 'is_AKI', 'is_Betablocker'])

pd.set_option('display.max_rows', 500)
print(data.describe())
data.to_excel("/Users/joudsi/Desktop/output.xlsx")
data.to_csv("/Users/joudsi/Desktop/output.csv")
