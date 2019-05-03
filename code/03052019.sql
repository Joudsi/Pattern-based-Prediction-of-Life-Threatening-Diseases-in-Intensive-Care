set search_path to mimiciii;

DROP MATERIALIZED VIEW IF EXISTS data_sepsis CASCADE;
CREATE MATERIALIZED VIEW data_sepsis as
(
	select final_data.*, 
		angus_sepsis.infection, 
		angus_sepsis.explicit_sepsis, 
		angus_sepsis.organ_dysfunction, 
		angus_sepsis.mech_vent,
		angus_sepsis.angus
	FROM final_data 
		INNER JOIN angus_sepsis 
		ON angus_sepsis.subject_id = final_data.subject_id
);

DROP MATERIALIZED VIEW IF EXISTS data_icd CASCADE;
CREATE MATERIALIZED VIEW data_icd as
(
SELECT distinct on (subject_id) ds.subject_id,
		ds.hadm_id,
		ds.icustay_id,
		ds.admission_age,
		ds.gender,
		ds.dod,
		ds.admission_type,
		ds.curr_service,
		ds.thirty_day_mort,
		ds.hr,
		ds.map,
		ds.sbp,
		ds.temp,
		ds.spo2,
		ds.rr,
		ds.cr,
		ds.k,
		ds.na,
		ds.cl,
		ds.bicarb,
		ds.hct,
		ds.wbc,
		ds.glucose,
		ds.mg,
		ds.ca,
		ds.p,
		ds.lactate,
		ds.infection, 
		ds.explicit_sepsis, 
		ds.organ_dysfunction, 
		ds.mech_vent,
		ds.angus,
		dicd.icd9_code
FROM data_sepsis ds
Inner JOIN diagnoses_icd dicd
  ON ds.subject_id = dicd.subject_id
);


DROP MATERIALIZED VIEW IF EXISTS data_angus_sepsis CASCADE;
CREATE MATERIALIZED VIEW data_angus_sepsis as
(
SELECT * FROM data_icd
where angus = 1
);


DROP MATERIALIZED VIEW IF EXISTS data_prescriptions CASCADE;
CREATE MATERIALIZED VIEW data_prescriptions as
(
	SELECT das.*, pres.drug_name_generic
	FROM data_angus_sepsis as das
	Left JOIN Prescriptions as pres
  	ON das.subject_id = pres.subject_id
)

select * from data_prescriptions;


-- counts of bb
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
	   
-- counts of AKI
select count(subject_id) from data_prescriptions
where icd9_code like '%5849%';


-- flagging BB and AKI (1 = true, 0 = false)

DROP MATERIALIZED VIEW IF EXISTS data_aki_bb CASCADE;
CREATE MATERIALIZED VIEW data_aki_bb as
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
        as BetaBlocker,
  Case when icd9_code like '%5849%' Then 1
  Else 0 End
  as AKI
  from co
  
-- counting people who takes bb and have AKI
select count(*) from data_aki_bb
where betablocker = 1 and aki = 1;


-- counting people who doesn't take bb and have AKI
select count(*) from data_aki_bb
where betablocker = 0 and aki = 1;

-- export to desktop
COPY (select * from data_aki_bb)
TO '/Users/joudsi/Desktop/data_aki_bb.csv' DELIMITER ',' CSV HEADER;

