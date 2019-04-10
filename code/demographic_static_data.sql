with dempgraphic_static_data as
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
 from mimiciii.icustays icu
 left join mimiciii.admissions adm on icu.hadm_id=adm.hadm_id
 left join mimiciii.patients pat on icu.subject_id=pat.subject_id
 left join mimiciii.services serv on icu.hadm_id=serv.hadm_id
 left join mimiciii.transfers trans on icu.hadm_id=trans.hadm_id
				
 where EXTRACT(EPOCH FROM (adm.admittime - pat.dob))/60.0/60.0/24.0/365.242 > 15 
   and trans.prev_careunit is null
   and icu.icustay_id is not null
)
select * from dempgraphic_static_data;