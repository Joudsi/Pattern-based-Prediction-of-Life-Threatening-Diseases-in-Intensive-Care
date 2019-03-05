set search_path to mimiciii;


DROP MATERIALIZED VIEW IF EXISTS cohort1 CASCADE;
CREATE MATERIALIZED VIEW cohort1 as
 WITH co AS (
         SELECT icu.subject_id,
            icu.hadm_id,
            icu.icustay_id,
            date_part('epoch'::text, icu.outtime - icu.intime) / 60.0::double precision / 60.0::double precision / 24.0::double precision AS icu_length_of_stay,
            date_part('epoch'::text, icu.intime - pat.dob) / 60.0::double precision / 60.0::double precision / 24.0::double precision / 365.242::double precision AS age,
            pat.gender,
                CASE
                    WHEN pat.gender::text ~~ 'F'::text THEN 1
                    ELSE 0
                END AS numeric_gender
           FROM mimiciii.icustays icu
             JOIN mimiciii.patients pat ON icu.subject_id = pat.subject_id
        ), serv AS (
         SELECT icu.hadm_id,
            icu.icustay_id,
            ad.language,
            ad.religion,
            ad.marital_status,
            ad.ethnicity,
			ad.diagnosis
           FROM mimiciii.icustays icu
             LEFT JOIN mimiciii.admissions ad ON icu.hadm_id = ad.hadm_id
        )
 SELECT co.subject_id,
    co.hadm_id,
    co.icustay_id,
    co.icu_length_of_stay,
    co.age,
    co.gender,
    co.numeric_gender,
    serv.language,
    serv.religion,
    serv.marital_status,
    serv.ethnicity,
	serv.diagnosis
   FROM co
     LEFT JOIN serv ON co.icustay_id = serv.icustay_id;


DROP MATERIALIZED VIEW IF EXISTS cohort1_distinct_rows CASCADE;
CREATE MATERIALIZED VIEW cohort1_distinct_rows as
SELECT DISTINCT ON (subject_id) subject_id, hadm_id, icustay_id, icu_length_of_stay, age, gender, numeric_gender, language, religion, marital_status, ethnicity, diagnosis
 from cohort1;
 

cohort1_distinct_rows wl diagnoses_icd
DROP MATERIALIZED VIEW IF EXISTS cohort1_diagnosis CASCADE;
CREATE MATERIALIZED VIEW cohort1_diagnosis as

SELECT distinct on (subject_id) dicd.icd9_code, c1d.*
FROM cohort1_distinct_rows c1d 
Inner JOIN diagnoses_icd dicd
  ON c1d.subject_id = dicd.subject_id;

select count(*) From diagnoses_icd;


select count(*) from cohort1_diagnosis;
select count(distinct subject_id) from cohort1_diagnosis;

select count(distinct(subject_id)) from cohort1_diagnosis
where icd9_code like '%5849%';
					  
select count(subject_id) from cohort1_diagnosis
where icd9_code like '%5849%'; 
					  
					  
					  
					  
COPY (select * from cohort1_diagnosis)
TO '/Users/joudsi/Desktop/initial_cohort.csv' DELIMITER ',' CSV HEADER;

select count(distinct hadm_id)
from angus_sepsis;

select count(*)
from angus_sepsis;					  

DROP MATERIALIZED VIEW IF EXISTS admissions_cohort1 CASCADE;
CREATE MATERIALIZED VIEW admissions_cohort1 as
WITH co AS
(
SELECT cohort1.*, angus.infection, angus.explicit_sepsis, angus.organ_dysfunction, angus.mech_vent, angus
FROM cohort1 
Inner JOIN angus_sepsis angus
  ON angus.hadm_id = cohort1.hadm_id
)
select co.*
from co
where co.angus = 1;
					  
COPY (select * from admissions_cohort1)
TO '/Users/joudsi/Desktop/admissions_cohort1.csv' DELIMITER ',' CSV HEADER;
					  
-- hoon mtl l 2abla bs lal cohort1 bs m3 diagnoses
DROP MATERIALIZED VIEW IF EXISTS admissions_cohort1_diagnosis CASCADE;
CREATE MATERIALIZED VIEW admissions_cohort1_diagnosis as
WITH co AS
(
SELECT cohort1_diagnosis.*, angus.infection, angus.explicit_sepsis, angus.organ_dysfunction, angus.mech_vent, angus
FROM cohort1_diagnosis
Inner JOIN angus_sepsis angus
  ON angus.hadm_id = cohort1_diagnosis.hadm_id
)
select co.*
from co
where co.angus = 1;	

					  
select count(*) from admissions_cohort1_diagnosis;					  

COPY (select * from admissions_cohort1_diagnosis)
TO '/Users/joudsi/Desktop/admissions_cohort1_diagnosis.csv' DELIMITER ',' CSV HEADER;
					  

select count(*) from admissions_cohort1_diagnosis;
select count(distinct(subject_id)) from admissions_cohort1_diagnosis;
select count(*) from admissions_cohort1;
					  


select count(*) From prescriptions;
					  
DROP MATERIALIZED VIEW IF EXISTS admissions_prescriptions_distinct CASCADE;
CREATE MATERIALIZED VIEW admissions_prescriptions_distinct as
SELECT acd.*, pres.startdate, pres.enddate, pres.drug_name_generic
FROM admissions_cohort1_diagnosis as acd
Left JOIN Prescriptions as pres
  ON acd.subject_id = pres.subject_id

select count(*) From admissions_prescriptions_distinct;
select count(*) From prescriptions;					  
					  

select count(subject_id)
From admissions_prescriptions_distinct
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
					  


select count(subject_id) from admissions_prescriptions_distinct
where icd9_code like '%5849%';
					  
DROP MATERIALIZED VIEW IF EXISTS admissions_prescriptions_distinct_withExclusions_bb_AKI CASCADE;
CREATE MATERIALIZED VIEW admissions_prescriptions_distinct_withExclusions_bb_AKI as
with co as (
	select apd.*
	from admissions_prescriptions_distinct apd
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
	then 0
    ELSE 1 END
        as exclusion_bb_drug,
  Case when icd9_code like '%5849%' Then 0
  Else 1 End
  as Exclusion_AKI
  from co
					  
COPY (select * from admissions_prescriptions_distinct_withExclusions_bb_AKI)
TO '/Users/joudsi/Desktop/admissions_prescriptions_distinct_withExclusions_bb_AKI.csv' DELIMITER ',' CSV HEADER;
					  
select count(*) from admissions_prescriptions_distinct_withExclusions_bb_AKI where exclusion_bb_drug = 0;
select count(*) from admissions_prescriptions_distinct_withExclusions_bb_AKI where Exclusion_AKI = 0;


DROP MATERIALIZED VIEW IF EXISTS admissions_prescriptions CASCADE;
CREATE MATERIALIZED VIEW admissions_prescriptions as
SELECT ac.*, pres.startdate, pres.enddate, pres.drug_name_generic
FROM admissions_cohort1 as ac
Left JOIN Prescriptions as pres
  ON ac.subject_id = pres.subject_id
					  

select count(subject_id)
From admissions_prescriptions
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
					  

select count(distinct(ap.subject_id)) 
from admissions_prescriptions ap
Inner join diagnoses_icd dicd
on ap.subject_id = dicd.subject_id					  
where icd9_code like '%5849%';
					  