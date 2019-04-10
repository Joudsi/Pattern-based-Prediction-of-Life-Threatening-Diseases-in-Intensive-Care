SET search_path TO mimiciii;

DROP MATERIALIZED VIEW if exists vitals;
DROP MATERIALIZED VIEW if exists vitals_raw;
DROP MATERIALIZED VIEW if exists small_chartevents;
DROP MATERIALIZED VIEW if exists static_data;

CREATE MATERIALIZED VIEW static_data
AS
select icu.subject_id,
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
						  ;

-- select * from static_data;

						   

CREATE MATERIALIZED VIEW small_chartevents	
AS		   
select icustay_id,
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
						   ;

-- select * from small_chartevents;


CREATE MATERIALIZED VIEW vitals_raw	
AS			
(select distinct icustay_id,        
        type,
        first_value(valuenum) over (partition by icustay_id, type order by charttime) as first_value
 from small_chartevents 
);

--select * from vitals_raw;
DROP extension tablefunc;					   
CREATE extension tablefunc;						   
						   
CREATE MATERIALIZED VIEW vitals	
AS									   
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
);

-- select * from vitals;
						   
						   