SET search_path TO mimiciii;
Drop  extension tablefunc;
CREATE extension tablefunc;

with static_data as
(select icu.subject_id,
        icu.hadm_id,
        icu.icustay_id,
 		case
         when ROUND( (CAST(EXTRACT(epoch FROM adm.admittime - pat.dob)/(60*60*24*365.242) AS numeric)), 4) > 150 then 91.4
         else ROUND( (CAST(EXTRACT(epoch FROM adm.admittime - pat.dob)/(60*60*24*365.242) AS numeric)), 4)
        end as admission_age,
		pat.gender,
		pat.dod,
 		adm.admission_type,
		serv.curr_service,
        case
         when adm.hospital_expire_flag = 1 or EXTRACT(epoch FROM (pat.dod-adm.dischtime))/(3600*24*30) < 12  then 'Y'		
         else 'N'
        end as thirty_day_mort
 from icustays icu
 left join admissions adm on icu.hadm_id=adm.hadm_id
 left join patients pat on icu.subject_id=pat.subject_id
 left join services serv on icu.hadm_id=serv.hadm_id
 left join transfers trans on icu.hadm_id=trans.hadm_id
				
 where EXTRACT(EPOCH FROM (adm.admittime - pat.dob))/60.0/60.0/24.0/365.242 > 15 
   and trans.prev_careunit is null
   and icu.icustay_id is not null
)
--select * from static_data;
			   
, small_chartevents as
(select icustay_id,
        case
         when itemid in (211) then 'hr'
         when itemid in (52,456) then 'map'  -- invasive and noninvasive measurements are combined
         when itemid in (51,455) then 'sbp'  -- invasive and noninvasive measurements are combined
         when itemid in (678,679) then 'temp'  -- in Fahrenheit
         when itemid in (646) then 'spo2'     
         when itemid in (618) then 'rr'
        end as type,                
        charttime,
        valuenum
 from chartevents l
 where itemid in (211,51,52,455,456,678,679,646,618)
   and icustay_id in (select icustay_id from static_data) 
   and valuenum is not null
)
-- select * from small_chartevents;

, vitals_raw as
(select distinct icustay_id,        
        type,
        first_value(valuenum) over (partition by icustay_id, type order by charttime) as first_value
 from small_chartevents 
)
-- select * from vitals_raw;
, vitals as
(
	SELECT * 
	FROM crosstab('select icustay_id, type, SUM (ROUND(first_value::numeric, 1)) from vitals_raw group by 1,2') 
		 AS final_result(
		icustay_id integer,
        hr  NUMERIC,
        map  NUMERIC,
        sbp  NUMERIC,
        temp  NUMERIC,
        spo2  NUMERIC,
        rr NUMERIC
		 )
)
--select * from vitals;
			   
, small_labevents as
(select  icu.icustay_id,
 		le.charttime,
		CASE
			WHEN le.itemid = 50868 THEN 'ANION_GAP'::text
			WHEN le.itemid = 50862 THEN 'ALBUMIN'::text
			WHEN le.itemid = 51144 THEN 'BANDS'::text
			WHEN le.itemid = 50882 THEN 'BICARBONATE'::text
			WHEN le.itemid = 50885 THEN 'BILIRUBIN'::text
			WHEN le.itemid = 50912 THEN 'CREATININE'::text
			WHEN le.itemid = 50806 THEN 'CHLORIDE'::text
			WHEN le.itemid = 50902 THEN 'CHLORIDE'::text
			WHEN le.itemid = 50809 THEN 'GLUCOSE'::text
			WHEN le.itemid = 50931 THEN 'GLUCOSE'::text
			WHEN le.itemid = 50810 THEN 'HEMATOCRIT'::text
			WHEN le.itemid = 51221 THEN 'HEMATOCRIT'::text
			WHEN le.itemid = 50811 THEN 'HEMOGLOBIN'::text
			WHEN le.itemid = 51222 THEN 'HEMOGLOBIN'::text
			WHEN le.itemid = 50813 THEN 'LACTATE'::text
			WHEN le.itemid = 51265 THEN 'PLATELET'::text
			WHEN le.itemid = 50822 THEN 'POTASSIUM'::text
			WHEN le.itemid = 50971 THEN 'POTASSIUM'::text
			WHEN le.itemid = 51275 THEN 'PTT'::text
			WHEN le.itemid = 51237 THEN 'INR'::text
			WHEN le.itemid = 51274 THEN 'PT'::text
			WHEN le.itemid = 50824 THEN 'SODIUM'::text
			WHEN le.itemid = 50983 THEN 'SODIUM'::text
			WHEN le.itemid = 51006 THEN 'BUN'::text
			WHEN le.itemid = 51300 THEN 'WBC'::text
			WHEN le.itemid = 51301 THEN 'WBC'::text
			ELSE NULL::text
		END AS label,
		CASE
			WHEN le.itemid = 50862 AND le.valuenum > 10::double precision THEN NULL::double precision
			WHEN le.itemid = 50868 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 51144 AND le.valuenum < 0::double precision THEN NULL::double precision
			WHEN le.itemid = 51144 AND le.valuenum > 100::double precision THEN NULL::double precision
			WHEN le.itemid = 50882 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50885 AND le.valuenum > 150::double precision THEN NULL::double precision
			WHEN le.itemid = 50806 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50902 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50912 AND le.valuenum > 150::double precision THEN NULL::double precision
			WHEN le.itemid = 50809 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50931 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50810 AND le.valuenum > 100::double precision THEN NULL::double precision
			WHEN le.itemid = 51221 AND le.valuenum > 100::double precision THEN NULL::double precision
			WHEN le.itemid = 50811 AND le.valuenum > 50::double precision THEN NULL::double precision
			WHEN le.itemid = 51222 AND le.valuenum > 50::double precision THEN NULL::double precision
			WHEN le.itemid = 50813 AND le.valuenum > 50::double precision THEN NULL::double precision
			WHEN le.itemid = 51265 AND le.valuenum > 10000::double precision THEN NULL::double precision
			WHEN le.itemid = 50822 AND le.valuenum > 30::double precision THEN NULL::double precision
			WHEN le.itemid = 50971 AND le.valuenum > 30::double precision THEN NULL::double precision
			WHEN le.itemid = 51275 AND le.valuenum > 150::double precision THEN NULL::double precision
			WHEN le.itemid = 51237 AND le.valuenum > 50::double precision THEN NULL::double precision
			WHEN le.itemid = 51274 AND le.valuenum > 150::double precision THEN NULL::double precision
			WHEN le.itemid = 50824 AND le.valuenum > 200::double precision THEN NULL::double precision
			WHEN le.itemid = 50983 AND le.valuenum > 200::double precision THEN NULL::double precision
			WHEN le.itemid = 51006 AND le.valuenum > 300::double precision THEN NULL::double precision
			WHEN le.itemid = 51300 AND le.valuenum > 1000::double precision THEN NULL::double precision
			WHEN le.itemid = 51301 AND le.valuenum > 1000::double precision THEN NULL::double precision
			ELSE le.valuenum
		END AS valuenum
   FROM icustays icu
	 LEFT JOIN labevents le ON le.subject_id = icu.subject_id AND le.hadm_id = icu.hadm_id AND le.charttime >= (icu.intime - '06:00:00'::interval hour) AND le.charttime <= (icu.intime + '1 day'::interval day) AND (le.itemid = ANY (ARRAY[50868, 50862, 51144, 50882, 50885, 50912, 50902, 50806, 50931, 50809, 51221, 50810, 51222, 50811, 50813, 51265, 50971, 50822, 51275, 51237, 51274, 50983, 50824, 51006, 51301, 51300])) AND le.valuenum IS NOT NULL AND le.valuenum > 0::double precision
	 and icustay_id in (select icustay_id from static_data)
)
-- select * from small_labevents;

, labs_raw as
(select distinct icustay_id,        
        label,
        first_value(valuenum) over (partition by icustay_id, label order by charttime) as first_value
 from small_labevents 
)
-- select * from labs_raw;

, labs as
(
    SELECT * 
	FROM crosstab('select icustay_id, label, SUM (ROUND(first_value::numeric, 1)) from labs_raw group by 1,2')     
			AS final_result(icustay_id integer,
       		ANION_GAP NUMERIC,
			ALBUMIN NUMERIC,
			BANDS NUMERIC,
			BICARBONATE NUMERIC,
			BILIRUBIN NUMERIC,
			CREATININE NUMERIC,
			CHLORIDE NUMERIC,
			GLUCOSE NUMERIC,
			HEMATOCRIT NUMERIC,
			HEMOGLOBIN NUMERIC,
			LACTATE NUMERIC,
			PLATELET NUMERIC,
			POTASSIUM NUMERIC,
			PTT NUMERIC,
			INR NUMERIC,
			PT NUMERIC,
			SODIUM NUMERIC,
			BUN NUMERIC,
			WBC NUMERIC
       )
      )

select * from labs;

