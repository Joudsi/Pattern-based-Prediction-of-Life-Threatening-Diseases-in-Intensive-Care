set search_path to mimiciii;

DROP MATERIALIZED VIEW IF EXISTS cohort1 CASCADE;
CREATE MATERIALIZED VIEW cohort1 as
select * from final_data;

DROP MATERIALIZED VIEW IF EXISTS data_distinct CASCADE; --cohort1_distinct_rows
CREATE MATERIALIZED VIEW data_distinct as
SELECT DISTINCT ON (subject_id) subject_id,
    hadm_id,
    icustay_id,
    admission_age,
    gender,
    dod,
    admission_type,
    curr_service,
    thirty_day_mort,
    hr,
    map,
    sbp,
    temp,
    spo2,
    rr,
    cr,
    k,
    na,
    cl,
    bicarb,
    hct,
    wbc,
    glucose,
    mg,
    ca,
    p,
    lactate
 
 from final_data;
 

DROP MATERIALIZED VIEW IF EXISTS data_diagnosis CASCADE; -- cohort1_diagnosis
CREATE MATERIALIZED VIEW data_diagnosis as

SELECT distinct on (subject_id) dicd.icd9_code, data_distinct.*
FROM data_distinct 
Inner JOIN diagnoses_icd dicd
  ON data_distinct.subject_id = dicd.subject_id;


DROP MATERIALIZED VIEW IF EXISTS data_angus CASCADE; --admissions_cohort1
CREATE MATERIALIZED VIEW data_angus as
WITH co AS
(
SELECT data_diagnosis.*, angus.infection, angus.explicit_sepsis, angus.organ_dysfunction, angus.mech_vent, angus
FROM data_diagnosis 
Inner JOIN angus_sepsis angus
  ON angus.hadm_id = data_diagnosis.hadm_id
)
select co.*
from co
where co.angus = 1;
								
					  
DROP MATERIALIZED VIEW IF EXISTS data_prescriptions CASCADE;
CREATE MATERIALIZED VIEW data_prescriptions as
SELECT acd.*, pres.startdate, pres.enddate, pres.drug_name_generic
FROM data_angus as acd
Left JOIN Prescriptions as pres
  ON acd.subject_id = pres.subject_id


select count(subject_id)
From data_prescriptions
where ( drug_name_generic LIKE 'Carvedilol%' 
	   OR drug_name_generic LIKE 'Metoprolol%' 
	   OR drug_name_generic LIKE 'Atenolol%' 
	  OR drug_name_generic LIKE 'Nadolol%'
	  OR drug_name_generic LIKE 'Nebivolol%'
	  OR drug_name_generic LIKE 'Propranolol'
	  OR drug_name_generic LIKE'Betaxolol' 
	  OR drug_name_generic LIKE 'Bisoprolol'
	   OR drug_name_generic LIKE 'Carteolol'
	   OR drug_name_generic LIKE 'Carvedilol'
	   OR drug_name_generic LIKE 'Labetalol'
	   OR drug_name_generic LIKE 'Nebivolol'
	   OR drug_name_generic LIKE 'Penbutolol'
	   OR drug_name_generic LIKE 'Pindolol'
	   OR drug_name_generic LIKE 'Sotalol'
	   OR drug_name_generic LIKE 'Timolol');
					  


select count(subject_id) from data_prescriptions
where icd9_code like '%5849%';
					  
DROP MATERIALIZED VIEW IF EXISTS data_angus_final CASCADE;
CREATE MATERIALIZED VIEW data_angus_final as
with co as (
	select data_prescriptions.*
	from data_prescriptions 
)
  select co.* ,
  CASE
        WHEN drug_name_generic like 'Carvedilol%' 
		OR drug_name_generic LIKE 'Metoprolol%' 
		OR drug_name_generic LIKE 'Atenolol%' 
	 	OR drug_name_generic LIKE 'Nadolol%'
	  	OR drug_name_generic LIKE 'Nebivolol%'
	  	OR drug_name_generic LIKE 'Propranolol'
	  	OR drug_name_generic LIKE'Betaxolol' 
	  	OR drug_name_generic LIKE 'Bisoprolol'
	   	OR drug_name_generic LIKE 'Carteolol'
	   	OR drug_name_generic LIKE 'Carvedilol'
	   	OR drug_name_generic LIKE 'Labetalol'
	   	OR drug_name_generic LIKE 'Nebivolol'
	   	OR drug_name_generic LIKE 'Penbutolol'
	   	OR drug_name_generic LIKE 'Pindolol'
	   	OR drug_name_generic LIKE 'Sotalol'
	   	OR drug_name_generic LIKE 'Timolol'
	then 1
    ELSE 0 END
        as exclusion_bb_drug,
  Case when icd9_code like '%5849%' Then 1
  Else 0 End
  as Exclusion_AKI
  from co