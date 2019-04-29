Set search_path to mimiciii;
DROP MATERIALIZED VIEW if exists labs_raw;

CREATE MATERIALIZED VIEW labs_raw
AS
with small_labevents as
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
select * from labs_raw;