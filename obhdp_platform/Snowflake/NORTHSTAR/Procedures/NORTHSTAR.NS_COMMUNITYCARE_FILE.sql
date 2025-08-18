CREATE OR REPLACE PROCEDURE ECT_PRD_OBH_DP_DB.NORTHSTAR.NS_COMMUNITYCARE_FILE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
    try
    {
SQLcmdNSInsert="insert into NORTHSTAR.NS_PROGRAM_INSERT_CMBD select  distinct  trim(client_patient_id) as Altruista_ID,'Program Enrollment' LOB,'Enrollment' as BENEFIT_PLAN,'NorthStar Comprehensive BH Program' PROGRAM_NAME,to_char(to_date(Case when REC_UPDT_DTTM is not null then REC_UPDT_DTTM else REC_CRT_DTTM end)  ,'MM/DD/YYYY') as START_DATE,to_char(to_date('2099-12-31'),'MM/DD/YYYY') as END_DATE,case when SEGMENT in ('high acuity') then 'Optum BH - High Risk' when SEGMENT in ('medium acuity') then 'Optum BH - Medium Risk' when SEGMENT in ('low acuity') then 'Optum BH - Low Risk' else 'Optum BH'  end as PROGRAM_REFERRAL_SOURCE,'Enrolled' as PROGRAM_STATUS,'Auto Enrolled' as PROGRAM_STATUS_DESC from  ( select distinct src_mbr_id,index_id,REC_UPDT_DTTM,REC_CRT_DTTM,SEGMENT,pd.client_patient_id  from (select * from(select ns.*,pi.INDEX_ID, PI.PATIENT_ID pi_patient_iD,pi.updated_on,row_number() over (partition by pi.INDEX_ID,src_mbr_id order by pi.UPDATED_ON DESC NULLS LAST) seq from NORTHSTAR.NS_MEMBER NS INNER JOIN COMPACT.patient_index pi ON  pi.index_value=NS.MEDICAID_NO  and pi.INDEX_ID = 10071 )where seq=1 order by src_mbr_id) inner join COMPACT.PATIENT_DETAILS PD where pi_patient_iD=PD.PATIENT_ID) UNION  select  distinct trim(client_patient_id) as Altruista_ID,'Program Enrollment' LOB,'Enrollment' as BENEFIT_PLAN,case when AE_NM like '%IBHH' then 'Health Home - Integrated'  when AE_NM='FIRSTHAND' then 'External Care Navigation' when AE_NM='FSP' then 'NorthStar BH - Family Support' when AE_NM='CARE NAVIGATION' then 'NorthStar BH - Care Navigation' end PROGRAM_NAME,to_char(to_date(Case when REC_UPDT_DTTM is not null then REC_UPDT_DTTM else  REC_CRT_DTTM end)  ,'MM/DD/YYYY') as START_DATE,   to_char(to_date('2099-12-31'),'MM/DD/YYYY') as END_DATE,case when SEGMENT in ('high acuity') then 'Optum BH - High Risk' when SEGMENT in ('medium acuity') then 'Optum BH - Medium Risk' when SEGMENT in ('low acuity') then 'Optum BH - Low Risk' else 'Optum BH'  end as PROGRAM_REFERRAL_SOURCE,'Identified' as PROGRAM_STATUS,'Meets criteria - Pending engagement' as PROGRAM_STATUS_DESC from  ( select distinct src_mbr_id,AE_NM,REC_UPDT_DTTM,REC_CRT_DTTM,SEGMENT,pd.client_patient_id  from (select * from (select ns.*,pi.INDEX_ID, PI.PATIENT_ID pi_patient_iD,pi.updated_on,row_number() over (partition by pi.INDEX_ID,src_mbr_id order by pi.UPDATED_ON DESC NULLS LAST) seq from NORTHSTAR.NS_MEMBER NS INNER JOIN COMPACT.patient_index pi ON  pi.index_value=NS.MEDICAID_NO  and pi.INDEX_ID = 10071 )where seq=1 order by src_mbr_id) inner join COMPACT.PATIENT_DETAILS PD where pi_patient_iD=PD.PATIENT_ID) where AE_NM in  ('VOA-North IBHH','CARE NAVIGATION','FSP','UPWARD HEALTH IBHH','LIFESTREAM IBHH','FIRSTHAND','MERIDIAN IBHH','CHRYSALIS IBHH','GRACEPOINT IBHH','VOA-South IBHH')";
var stmtNSInsert = snowflake.createStatement({ sqlText: SQLcmdNSInsert} );
var resultNSInsert = stmtNSInsert.execute();
resultNSInsert.next();


SQLcmdNSInserttemp="create or replace temporary table NORTHSTAR.MBR_INC AS (select distinct src_mbr_id from NORTHSTAR.NS_MEMBER NS minus(select distinct src_mbr_id  from (select * from (select ns.*,pi.INDEX_ID, PI.PATIENT_ID pi_patient_iD,pi.updated_on,row_number() over (partition by pi.INDEX_ID,src_mbr_id order by pi.UPDATED_ON DESC NULLS LAST) seq from NORTHSTAR.NS_MEMBER NS INNER JOIN COMPACT.patient_index pi ON  pi.index_value=NS.MEDICAID_NO  and pi.INDEX_ID = 10071 order  by src_mbr_id )where seq=1 order by src_mbr_id) inner join COMPACT.PATIENT_DETAILS PD where pi_patient_iD=PD.PATIENT_ID) )";
var stmtNSInserttemp = snowflake.createStatement({ sqlText: SQLcmdNSInserttemp} );
var resultNSInserttemp = stmtNSInserttemp.execute();
resultNSInserttemp.next();

SQLcmdNSInsert1="insert into NORTHSTAR.NS_PROGRAM_INSERT_CMBD select  distinct trim(client_patient_id) as Altruista_ID,'Program Enrollment' LOB,'Enrollment' as BENEFIT_PLAN,'NorthStar Comprehensive BH Program' PROGRAM_NAME,to_char(to_date(Case when REC_UPDT_DTTM is not null then REC_UPDT_DTTM else REC_CRT_DTTM end)  ,'MM/DD/YYYY') as START_DATE,to_char(to_date('2099-12-31'),'MM/DD/YYYY') as END_DATE,case when SEGMENT in ('high acuity') then 'Optum BH - High Risk' when SEGMENT in ('medium acuity') then 'Optum BH - Medium Risk' when SEGMENT in ('low acuity') then 'Optum BH - Low Risk' else 'Optum BH'  end as PROGRAM_REFERRAL_SOURCE,'Enrolled' as PROGRAM_STATUS,'Auto Enrolled' as PROGRAM_STATUS_DESC from   ( select distinct src_mbr_id,index_id,REC_UPDT_DTTM,REC_CRT_DTTM,SEGMENT,pd.client_patient_id  from (select * from (select ns.*,pi.INDEX_ID, PI.PATIENT_ID pi_patient_iD,pi.updated_on,row_number() over (partition by pi.INDEX_ID,src_mbr_id order by pi.UPDATED_ON DESC NULLS LAST) seq  from NORTHSTAR.NS_MEMBER NS INNER JOIN COMPACT.patient_index pi ON  pi.index_value=NS.SUBSCRIBER_ID  and pi.INDEX_ID = 10073  and src_mbr_id in (select src_mbr_id from NORTHSTAR.MBR_INC))where seq=1 order by src_mbr_id) inner join COMPACT.PATIENT_DETAILS PD where pi_patient_iD=PD.PATIENT_ID)";
var stmtNSInsert1 = snowflake.createStatement({ sqlText: SQLcmdNSInsert1} );
var resultNSInsert1 = stmtNSInsert1.execute();
resultNSInsert1.next();

SQLcmdNSInsert2="insert into NORTHSTAR.NS_PROGRAM_INSERT_CMBD  select  distinct trim(client_patient_id) as Altruista_ID,'Program Enrollment' LOB,'Enrollment' as BENEFIT_PLAN,case when AE_NM like '%IBHH' then 'Health Home - Integrated'  when AE_NM='FIRSTHAND' then 'External Care Navigation' when AE_NM='FSP' then 'NorthStar BH - Family Support' when AE_NM='CARE NAVIGATION' then 'NorthStar BH - Care Navigation' end PROGRAM_NAME,to_char(to_date(Case when REC_UPDT_DTTM is not null then REC_UPDT_DTTM else  REC_CRT_DTTM end)  ,'MM/DD/YYYY') as START_DATE,to_char(to_date('2099-12-31'),'MM/DD/YYYY') as END_DATE,case when SEGMENT in ('high acuity') then 'Optum BH - High Risk' when SEGMENT in ('medium acuity') then 'Optum BH - Medium Risk' when SEGMENT in ('low acuity') then 'Optum BH - Low Risk' else 'Optum BH'  end as PROGRAM_REFERRAL_SOURCE,'Identified' as PROGRAM_STATUS,'Meets criteria - Pending engagement' as PROGRAM_STATUS_DESC from  ( select distinct src_mbr_id,AE_NM,REC_UPDT_DTTM,REC_CRT_DTTM,SEGMENT,pd.client_patient_id  from (select * from (select ns.*,pi.INDEX_ID, PI.PATIENT_ID pi_patient_iD,pi.updated_on,row_number() over (partition by pi.INDEX_ID,src_mbr_id order by pi.UPDATED_ON DESC NULLS LAST) seq from NORTHSTAR.NS_MEMBER NS INNER JOIN COMPACT.patient_index pi ON  pi.index_value=NS.SUBSCRIBER_ID and pi.INDEX_ID = 10073 and src_mbr_id in (select src_mbr_id from NORTHSTAR.MBR_INC))where seq=1 order by src_mbr_id) inner join COMPACT.PATIENT_DETAILS PD where pi_patient_iD=PD.PATIENT_ID) where AE_NM in   ('VOA-North IBHH','CARE NAVIGATION','FSP','UPWARD HEALTH IBHH','LIFESTREAM IBHH','FIRSTHAND','MERIDIAN IBHH','CHRYSALIS IBHH','GRACEPOINT IBHH','VOA-South IBHH')";
var stmtNSInsert2 = snowflake.createStatement({ sqlText: SQLcmdNSInsert2} );
var resultNSInsert2 = stmtNSInsert2.execute();
resultNSInsert2.next();
var resultNSInsert2 = resultNSInsert2.getColumnValue(1);

result11=resultNSInsert2;
return result11;


  }
      catch (err) {
          result =  "Failed: Code: " + err.code + ", State: " + err.state;
          result += "\\n Message: " + err.message;
          result += "\\n Stack Trace:\\n" + err.stackTraceTxt;
          throw result;
         }
  ;