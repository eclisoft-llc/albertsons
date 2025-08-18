drop table FACT_CLAIM;

create or replace table FACT_CLAIM
(
ADJUD_DATE varchar(200),
ALLOWED_AMT varchar(200),
AUTH_NUMBER varchar(200),
BILL_TYPE_DIM_ID varchar(200),
BILLED_AMT varchar(200),
CALC_ADMITS varchar(200),
CALC_DAYS varchar(200),
CALC_PROCEDURES varchar(200),
CALC_UNITS varchar(200),
CALC_VISITS varchar(200),
CLAIM_NUMBER varchar(200),
CLAIM_STATUS_DIM_ID varchar(200),
CLAIM_THRU_DATE varchar(200),
CLAIM_TYPE_DIM_ID varchar(200),
CLASS_DIM_ID varchar(200),
COMPANY_DIM_ID varchar(200),
CONVERTED_FROM_DIM_ID varchar(200),
COPAYMENT_1_AMT varchar(200),
COPAYMENT_2_AMT varchar(200),
COS_STL_4_DIM_ID varchar(200),
CPT_CODE_DIM_ID varchar(200),
CPT_MODIFIER_DIM_ID varchar(200),
DETAIL_SVC_DATE varchar(200),
DIAGNOSIS_1_DIM_ID varchar(200),
DRG_CODE_DIM_ID varchar(200),
LINE_NUMBER varchar(200),
MEMB_DIM_ID varchar(200),
MEMBER_AGE varchar(200),
NET_AMT varchar(200),
NOT_COVERED_AMT varchar(200),
OTHER_CARRIER_AMT varchar(200),
PAID_NET_AMT varchar(200),
PLACE_OF_SVC_1_DIM_ID varchar(200),
PLAN_DIM_ID varchar(200),
POST_DATE varchar(200),
PRIMARY_SVC_DATE varchar(200),
PROV_DIM_ID varchar(200),
PROV_SPEC_DIM_ID varchar(200),
PROV_TYPE_DIM_ID varchar(200),
PROVIDER_PAR_DIM_ID varchar(200),
QUANTITY varchar(200),
REVENUE_CODE_DIM_ID varchar(200),
SEQ_CLAIM_ID varchar(200),
SUB_LINE_CODE varchar(200),
SVC_TO_DATE varchar(200),
VEND_DIM_ID varchar(200),
WHOLE_CLAIM_STATUS_DIM_ID varchar(200),
ADJ_MM varchar(200));


/* create STAGE & File format */
create or replace file format fact_mycsvformat
  type = 'CSV'
  COMPRESSION = GZIP
  field_delimiter = '|'
  skip_header = 1
  record_delimiter='\n';
//Creating landing space for CSV
create or replace stage fact_my_csv_stage
  file_format = fact_mycsvformat;

/* Validate FACT STAGE files */
  ls @fact_my_csv_stage;
  /* LOAD FACT CLAIM*/
  copy into FACT_CLAIM
from @FACT_MY_CSV_STAGE/
file_format=fact_mycsvformat
pattern ='.*2021_02.*'
ON_ERROR = skip_file;
/* Validate the loaded records*/
select count(1) from FACT_CLAIM;
--13675222

select to_varchar(ADJUD_DATE ::date),count(1) from FACT_CLAIM group by  to_varchar(ADJUD_DATE ::date);

select top 100 * from FACT_CLAIM;
select distinct ADJUD_DATE from FACT_CLAIM;

/*select distinct ADJUD_DATE,TO_VARCHAR(ADJUD_DATE::DATE, 'YYYY-MM-DD') from FACT_CLAIM where length(ADJUD_DATE)=9 and
CLAIM_NUMBER IN (select distinct CLAIM_NUMBER from FACT_CLAIM where trim(ADJUD_DATE) <> '00:00:00' and length(ADJUD_DATE)=9);*/

select to_varchar('26-JAN-21'::date),to_date('26-JAN-21')
;
alter session set date_input_format = 'DD-MON-YY';
alter session set  TWO_DIGIT_CENTURY_START=2000;
select cast ('26-JAN-21' as date) from dual;

select distinct ADJUD_DATE,cast(ADJUD_DATE as date) ADJ_DD,count(1) from FACT_CLAIM  where
CLAIM_NUMBER IN (select distinct CLAIM_NUMBER from FACT_CLAIM where trim(ADJUD_DATE) <> '00:00:00' and trim(length(ADJUD_DATE))=9 and CLAIM_NUMBER not in (select distinct claim_number from FACT_CLAIM_M))
group by ADJUD_DATE,cast(ADJUD_DATE as date) ;

select ADJUD_DATE,count(1) from

select to_date('2021-03-03T00:00:00.000-06:00') as DATE1 from dual;

select SPLIT_PART('2021-03-03T00:00:00.000-06:00','T',1) as DATE1;

select distinct SPLIT_PART(ADJUD_DATE,'T',1) as DATE1,count(1)  from FACT_CLAIM where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9 group by DATE1  ;
select ADJUD_DATE,count(1) from FACT_CLAIM where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9 group by ADJUD_DATE ;

select *, case when CALC_ADMITS='0E-10' then '0' end CALC_ADMIT, case when CALC_DAYS='0E-10' then '0' end CALC_DAY, CALC_PROCEDURES , CALC_UNITS , CALC_VISITS
from FACT_CLAIM where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9 and SPLIT_PART(ADJUD_DATE,'T',1) ='2021-03-29';



select 'M' as tbl,
SPLIT_PART(ADJUD_DATE,'T',1) as ADJUD_DATE,
ALLOWED_AMT,
AUTH_NUMBER,
--cast(round(BILL_TYPE_DIM_ID) as integer) as BILL_TYPE_DIM_ID,
truncate(BILL_TYPE_DIM_ID) as BILL_TYPE_DIM_ID,
BILLED_AMT,
case when CALC_ADMITS='0E-10' then '0' else CALC_ADMITS end as CALC_ADMITS,
case when CALC_DAYS='0E-10' then '0' else CALC_DAYS end as CALC_DAYS,
case when CALC_PROCEDURES='0E-10' then '0' else CALC_PROCEDURES end as CALC_PROCEDURES,
case when CALC_UNITS='0E-10' then '0' else CALC_UNITS end as CALC_UNITS,
case when CALC_VISITS='0E-10' then '0' else CALC_VISITS end as CALC_VISITS,
CLAIM_NUMBER,
cast(CLAIM_STATUS_DIM_ID as integer) as CLAIM_STATUS_DIM_ID,
SPLIT_PART(CLAIM_THRU_DATE,'T',1) as CLAIM_THRU_DATE,
cast(CLAIM_TYPE_DIM_ID as integer) as CLAIM_TYPE_DIM_ID,
cast(CLASS_DIM_ID as integer) as CLASS_DIM_ID,
cast(COMPANY_DIM_ID as integer) as COMPANY_DIM_ID,
cast(CONVERTED_FROM_DIM_ID as integer) as CONVERTED_FROM_DIM_ID,
COPAYMENT_1_AMT ,
COPAYMENT_2_AMT,
cast(COS_STL_4_DIM_ID as integer) as COS_STL_4_DIM_ID,
cast(CPT_CODE_DIM_ID as integer) as CPT_CODE_DIM_ID,
cast(CPT_MODIFIER_DIM_ID as integer) as CPT_MODIFIER_DIM_ID,
SPLIT_PART(DETAIL_SVC_DATE,'T',1) as DETAIL_SVC_DATE,
cast(DIAGNOSIS_1_DIM_ID as integer) as DIAGNOSIS_1_DIM_ID,
cast(DRG_CODE_DIM_ID as integer) as DRG_CODE_DIM_ID,
cast(LINE_NUMBER as integer) as LINE_NUMBER,
cast(MEMB_DIM_ID as integer) as MEMB_DIM_ID,
MEMBER_AGE,
NET_AMT,
NOT_COVERED_AMT,
OTHER_CARRIER_AMT,
PAID_NET_AMT,
cast(PLACE_OF_SVC_1_DIM_ID as integer) as PLACE_OF_SVC_1_DIM_ID,
cast(PLAN_DIM_ID as integer) as PLAN_DIM_ID,
SPLIT_PART(POST_DATE,'T',1) as POST_DATE,
SPLIT_PART(PRIMARY_SVC_DATE,'T',1) as  PRIMARY_SVC_DATE,
cast(PROV_DIM_ID as integer) as PROV_DIM_ID,
cast(PROV_SPEC_DIM_ID as integer) as PROV_SPEC_DIM_ID,
cast(PROV_TYPE_DIM_ID as integer) as PROV_TYPE_DIM_ID,
cast(PROVIDER_PAR_DIM_ID as integer) as PROVIDER_PAR_DIM_ID,
QUANTITY,
cast(REVENUE_CODE_DIM_ID as integer) as REVENUE_CODE_DIM_ID,
cast(SEQ_CLAIM_ID as integer) as SEQ_CLAIM_ID,
SUB_LINE_CODE,
SPLIT_PART(SVC_TO_DATE,'T',1) as SVC_TO_DATE,
cast (VEND_DIM_ID as integer) as VEND_DIM_ID,
cast (WHOLE_CLAIM_STATUS_DIM_ID as integer) as WHOLE_CLAIM_STATUS_DIM_ID,
ADJ_MM
from FACT_CLAIM
where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9 and SPLIT_PART(ADJUD_DATE,'T',1) ='2021-03-29';
union all
select  'O' as tbl,
ADJUD_DATE,
ALLOWED_AMT,
AUTH_NUMBER,
BILL_TYPE_DIM_ID,
BILLED_AMT,
CALC_ADMITS,
CALC_DAYS,
CALC_PROCEDURES,
CALC_UNITS,
CALC_VISITS,
CLAIM_NUMBER,
CLAIM_STATUS_DIM_ID,
CLAIM_THRU_DATE,
CLAIM_TYPE_DIM_ID,
CLASS_DIM_ID,
COMPANY_DIM_ID,
CONVERTED_FROM_DIM_ID,
COPAYMENT_1_AMT,
COPAYMENT_2_AMT,
COS_STL_4_DIM_ID,
CPT_CODE_DIM_ID,
CPT_MODIFIER_DIM_ID,
DETAIL_SVC_DATE,
DIAGNOSIS_1_DIM_ID,
DRG_CODE_DIM_ID,
LINE_NUMBER,
MEMB_DIM_ID,
MEMBER_AGE,
NET_AMT,
NOT_COVERED_AMT,
OTHER_CARRIER_AMT,
PAID_NET_AMT,
PLACE_OF_SVC_1_DIM_ID,
PLAN_DIM_ID,
POST_DATE,
PRIMARY_SVC_DATE,
PROV_DIM_ID,
PROV_SPEC_DIM_ID,
PROV_TYPE_DIM_ID,
PROVIDER_PAR_DIM_ID,
QUANTITY,
REVENUE_CODE_DIM_ID,
SEQ_CLAIM_ID,
SUB_LINE_CODE,
SVC_TO_DATE,
VEND_DIM_ID,
WHOLE_CLAIM_STATUS_DIM_ID,
ADJ_MM FROM FACT_CLAIM
where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9 and SPLIT_PART(ADJUD_DATE,'T',1) ='2021-03-29';


create or replace table FACT_CLAIM_M
as
select
SPLIT_PART(ADJUD_DATE,'T',1) as ADJUD_DATE,
ALLOWED_AMT,
AUTH_NUMBER,
cast(round(BILL_TYPE_DIM_ID) as integer) as BILL_TYPE_DIM_ID,
BILLED_AMT,
case when CALC_ADMITS='0E-10' then '0' else CALC_ADMITS end as CALC_ADMITS,
case when CALC_DAYS='0E-10' then '0' else CALC_DAYS end as CALC_DAYS,
case when CALC_PROCEDURES='0E-10' then '0' else CALC_PROCEDURES end as CALC_PROCEDURES,
case when CALC_UNITS='0E-10' then '0' else CALC_UNITS end as CALC_UNITS,
case when CALC_VISITS='0E-10' then '0' else CALC_VISITS end as CALC_VISITS,
CLAIM_NUMBER,
cast(CLAIM_STATUS_DIM_ID as integer) as CLAIM_STATUS_DIM_ID,
SPLIT_PART(CLAIM_THRU_DATE,'T',1) as CLAIM_THRU_DATE,
cast(CLAIM_TYPE_DIM_ID as integer) as CLAIM_TYPE_DIM_ID,
cast(CLASS_DIM_ID as integer) as CLASS_DIM_ID,
cast(COMPANY_DIM_ID as integer) as COMPANY_DIM_ID,
cast(CONVERTED_FROM_DIM_ID as integer) as CONVERTED_FROM_DIM_ID,
COPAYMENT_1_AMT ,
COPAYMENT_2_AMT,
cast(COS_STL_4_DIM_ID as integer) as COS_STL_4_DIM_ID,
cast(CPT_CODE_DIM_ID as integer) as CPT_CODE_DIM_ID,
cast(CPT_MODIFIER_DIM_ID as integer) as CPT_MODIFIER_DIM_ID,
SPLIT_PART(DETAIL_SVC_DATE,'T',1) as DETAIL_SVC_DATE,
cast(DIAGNOSIS_1_DIM_ID as integer) as DIAGNOSIS_1_DIM_ID,
cast(DRG_CODE_DIM_ID as integer) as DRG_CODE_DIM_ID,
cast(LINE_NUMBER as integer) as LINE_NUMBER,
cast(MEMB_DIM_ID as integer) as MEMB_DIM_ID,
MEMBER_AGE,
NET_AMT,
NOT_COVERED_AMT,
OTHER_CARRIER_AMT,
PAID_NET_AMT,
cast(PLACE_OF_SVC_1_DIM_ID as integer) as PLACE_OF_SVC_1_DIM_ID,
cast(PLAN_DIM_ID as integer) as PLAN_DIM_ID,
SPLIT_PART(POST_DATE,'T',1) as POST_DATE,
SPLIT_PART(PRIMARY_SVC_DATE,'T',1) as  PRIMARY_SVC_DATE,
cast(PROV_DIM_ID as integer) as PROV_DIM_ID,
cast(PROV_SPEC_DIM_ID as integer) as PROV_SPEC_DIM_ID,
cast(PROV_TYPE_DIM_ID as integer) as PROV_TYPE_DIM_ID,
cast(PROVIDER_PAR_DIM_ID as integer) as PROVIDER_PAR_DIM_ID,
QUANTITY,
cast(REVENUE_CODE_DIM_ID as integer) as REVENUE_CODE_DIM_ID,
cast(SEQ_CLAIM_ID as integer) as SEQ_CLAIM_ID,
SUB_LINE_CODE,
SPLIT_PART(SVC_TO_DATE,'T',1) as SVC_TO_DATE,
cast (VEND_DIM_ID as integer) as VEND_DIM_ID,
cast (WHOLE_CLAIM_STATUS_DIM_ID as integer) as WHOLE_CLAIM_STATUS_DIM_ID,
ADJ_MM
from FACT_CLAIM
where ADJUD_DATE like '%T%' and trim(length(ADJUD_DATE))> 9

;
select count(1) from FACT_CLAIM_M;

select count(1) from FACT_CLAIM_M1; --114665864


create or replace table FACT_CLAIM_M1
as
select
cast(ADJUD_DATE as date) as ADJUD_DATE,
ALLOWED_AMT,
AUTH_NUMBER,
cast(round(BILL_TYPE_DIM_ID) as integer) as BILL_TYPE_DIM_ID,
BILLED_AMT,
case when CALC_ADMITS='0E-10' then '0' else CALC_ADMITS end as CALC_ADMITS,
case when CALC_DAYS='0E-10' then '0' else CALC_DAYS end as CALC_DAYS,
case when CALC_PROCEDURES='0E-10' then '0' else CALC_PROCEDURES end as CALC_PROCEDURES,
case when CALC_UNITS='0E-10' then '0' else CALC_UNITS end as CALC_UNITS,
case when CALC_VISITS='0E-10' then '0' else CALC_VISITS end as CALC_VISITS,
CLAIM_NUMBER,
cast(CLAIM_STATUS_DIM_ID as integer) as CLAIM_STATUS_DIM_ID,
cast(CLAIM_THRU_DATE as date) as CLAIM_THRU_DATE,
cast(CLAIM_TYPE_DIM_ID as integer) as CLAIM_TYPE_DIM_ID,
cast(CLASS_DIM_ID as integer) as CLASS_DIM_ID,
cast(COMPANY_DIM_ID as integer) as COMPANY_DIM_ID,
cast(CONVERTED_FROM_DIM_ID as integer) as CONVERTED_FROM_DIM_ID,
COPAYMENT_1_AMT ,
COPAYMENT_2_AMT,
cast(COS_STL_4_DIM_ID as integer) as COS_STL_4_DIM_ID,
cast(CPT_CODE_DIM_ID as integer) as CPT_CODE_DIM_ID,
cast(CPT_MODIFIER_DIM_ID as integer) as CPT_MODIFIER_DIM_ID,
cast(DETAIL_SVC_DATE as date) as DETAIL_SVC_DATE,
cast(DIAGNOSIS_1_DIM_ID as integer) as DIAGNOSIS_1_DIM_ID,
cast(DRG_CODE_DIM_ID as integer) as DRG_CODE_DIM_ID,
cast(LINE_NUMBER as integer) as LINE_NUMBER,
cast(MEMB_DIM_ID as integer) as MEMB_DIM_ID,
MEMBER_AGE,
NET_AMT,
NOT_COVERED_AMT,
OTHER_CARRIER_AMT,
PAID_NET_AMT,
cast(PLACE_OF_SVC_1_DIM_ID as integer) as PLACE_OF_SVC_1_DIM_ID,
cast(PLAN_DIM_ID as integer) as PLAN_DIM_ID,
cast(POST_DATE as date) as POST_DATE,
cast(PRIMARY_SVC_DATE as date) as  PRIMARY_SVC_DATE,
cast(PROV_DIM_ID as integer) as PROV_DIM_ID,
cast(PROV_SPEC_DIM_ID as integer) as PROV_SPEC_DIM_ID,
cast(PROV_TYPE_DIM_ID as integer) as PROV_TYPE_DIM_ID,
cast(PROVIDER_PAR_DIM_ID as integer) as PROVIDER_PAR_DIM_ID,
QUANTITY,
cast(REVENUE_CODE_DIM_ID as integer) as REVENUE_CODE_DIM_ID,
cast(SEQ_CLAIM_ID as integer) as SEQ_CLAIM_ID,
SUB_LINE_CODE,
cast(SVC_TO_DATE as date) as SVC_TO_DATE,
cast (VEND_DIM_ID as integer) as VEND_DIM_ID,
cast (WHOLE_CLAIM_STATUS_DIM_ID as integer) as WHOLE_CLAIM_STATUS_DIM_ID,
ADJ_MM
from FACT_CLAIM
where
CLAIM_NUMBER IN (select distinct CLAIM_NUMBER from FACT_CLAIM where trim(ADJUD_DATE) <> '00:00:00'
and trim(length(ADJUD_DATE))=9 and CLAIM_NUMBER not in (select distinct claim_number from FACT_CLAIM_M))  ;


select * from FACT_CLAIM where CLAIM_NUMBER not in
(
select distinct CLAIM_NUMBER from FACT_CLAIM_M1 union all select distinct CLAIM_NUMBER from FACT_CLAIM_M)
