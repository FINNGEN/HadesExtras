

Cohort definition 
A set of triplets patient_id, cohort_start_date, cohort_end_date
Where cohort_start_date < cohort_end_date
Periods cannot overlap

cohort algebra
Union : A U B, A OR B, 
patient-days that are either in A or in B, 
keep all periods, join overlaping periods into one


Intersect: A ^ B, A AND B, 
patient-days that are at the same time in both in A and in B, 
keep only the overlapping periods
- this is not good, imaging asthma ^ copd. If a patient has asthma firts and COPD later, the new onset day is the COPD, it is not preserving the asthma onset, that may be wanted
- this is not good. If pregnancy ^ diabetes


setDiff: A-B, A NOT IN B, 
patient-days that are in A but not in B, 
keep periods of A that not overlapp with B
- this is not good, imaging asthma - copd asthma happens first and copd later. This keeps all the asthma patients, these with pure asthma un touch, these with overlap the duration is shorten, This is not what people will expect. 


Suggested operators:

Union : Patients and days in either A or B
Intersect patient-days: Patients and days in A with patients and days in B
Intersect patients: Patients and days in A with patients in B
setDiff patient-days : Patients and days in A with patients and days in B
setDiff patients :



cohort set operations 

Union : A U B, A OR B, patient that are either in A or in B, keep all periods
Intersect: A ^ B, A AND B, patients that are at the same time in both in A and in B, keep all periods
setDiff: A-B, A NOT IN B, patients that are in A but not in B, keep all periods in A


periods operators

Decrease to one period per patient

- first to last events 
- first to first event 
- last to last event 






to get 
- algebra cohorts
- hadesextras






