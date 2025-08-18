CREATE OR REPLACE PROCEDURE NORTHSTAR.NS_MEMBER_DATATABLE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
declare result varchar;
begin
insert into NORTHSTAR.NS_MEMBER_DATATABLE (UHG_SEG_NM,STATE_ABBREVIATION,ACCOUNTABLE_ENTITY,BENE_FIRST_NAME,BENE_LAST_NAME,BENE_DOB,MEMB_ADDRESS_LINE_1,MEMB_CITY,
                                           MEMB_COUNTY,MEMB_STATE,MEMB_ZIP,LOB,MEDICAID_NO,MEMB_DIM_ID,	SUBSCRIBER_ID,SEGMENTATION,BH_COST_OF_CARE,MED_COST_OF_CARE,RX_COST_OF_CARE,
                                           TOTAL_COST_OF_CARE,MED_VISIT,ER_VISIT,BH_VISIT,PROGRAM_START_DATE,REC_CRT_DTTM,REC_UPDT_DTTM,MEMB_ELIG_STATUS,DOD,DIAMOND_ID,SEQ_MEMB_ID,HOME_PHONE_NUMBER,CELL_PHONE_NUMBER,RES_PARTY_EMAIL,
										   SOCIAL_SEC_NO
 )
select  'Community & State' UHG_SEG_NM,NS.STATE_CODE,NS.AE_NM,NS.MEMB_FIRST_NAME,NS.MEMB_LAST_NAME,NS.DOB,NS.MEMB_ADDRESS_LINE_1,NS.MEMB_CITY,NS.COUNTY,NS.MEMB_STATE,NS.MEMB_ZIP,NS.LOB,NS.MEDICAID_NO,NS.SRC_MBR_ID,
        NS.SUBSCRIBER_ID,NS.SEGMENT,NS.BH_COST_OF_CARE,NS.MED_COST_OF_CARE,NS.RX_COST_OF_CARE,NS.TOTAL_COST_OF_CARE,NS.MED_VISIT,NS.ER_VISIT,NS.BH_VISIT,NS.REC_CRT_DTTM program_start_date,
        NS.REC_CRT_DTTM,NS.REC_UPDT_DTTM,MBR.MEMB_ELIG_STATUS,MBR.DOD,MBR.DIAMOND_ID,MBR.SEQ_MEMB_ID,MBR.HOME_PHONE_NUMBER,MBR.CELL_PHONE_NUMBER,MBR.RES_PARTY_EMAIL,NS.SOCIAL_SEC_NO
from NORTHSTAR.NS_MEMBER NS inner JOIN COMPACT.DIM_MEMBER MBR ON NS.SRC_MBR_ID=MBR.MEMB_DIM_ID;


create or replace temporary table northstar.ns_imdm_missing as ( select memb_dim_id src_mbr_id,bene_first_name,bene_last_name,bene_DOB,SOCIAL_SEC_NO,SEQ_MEMB_ID,MEDICAID_NO
                                                                   from NORTHSTAR.NS_MEMBER_DATATABLE where indv_id is null );
create or replace temporary table northstar.ns_imdm as(select distinct final_indv_id indv_id1,src_mbr_id,MAX(IMDM_HOME_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_HOME_PHONE,
                                                       MAX(IMDM_CELL_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_CELL_PHONE,MAX(IMDM_UNKNOWN_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_UNKNOWN_PHONE
from (
select distinct indv_key.INDV_KEY_VAL,indv.INDV_ID,ns.src_mbr_id,NS.SEQ_MEMB_ID , NS.DOB, indv.BTH_DT,upper(indv.fst_nm)fst_nm,ns.memb_first_name,upper(indv.lst_nm) lst_nm,ns.memb_last_name,
indv.ssn,m.social_sec_no,case when TEL.CMNCT_TYP_DESC in ('Home Telephone') then TEL_NBR end IMDM_HOME_PHONE1,case when TEL.CMNCT_TYP_DESC in ('Mobile Telephone') then TEL_NBR end IMDM_CELL_PHONE1,
case when TEL.CMNCT_TYP_DESC in ('Unknown Telephone') then first_value(TEL_NBR) over (partition by indv.indv_id order by TEL.LST_UPDT_SRC_SYS_TMSTMP desc) end IMDM_UNKNOWN_PHONE1,
  HOME_PHONE_NUMBER,CELL_PHONE_NUMBER,RES_PARTY_EMAIL,
     INDV_KEY.DW_SYS_REF_CD,first_value(indv.indv_id) over (partition by indv_key.INDV_KEY_VAL order by  indv.PERS_NM_LST_UPDT_SRC_SYS_TMSTMP desc) final_indv_id
  from  ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV_KEY indv_key inner join ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV indv
  on  INDV.INDV_ID=INDV_KEY.INDV_ID  and INDV_KEY_TYP_REF_DESC='medicaidRecipientNumber'
 AND INDV_KEY.ACTV='true' and  INDV_KEY.DW_SYS_REF_CD='IMDM' and indv.NM_TYP_DESC='Preferred' and INDV.SRC_SYS_CD='IMDM'
   and INDV.ACTV='true' and  UPPER(INDV.DW_SRC_REC_STS_CD)='TRUE'
  inner join NORTHSTAR.NS_MEMBER NS on ns.medicaid_no=indv_key.INDV_KEY_VAL
  inner join northstar.ns_imdm_missing ns1 on ns1.src_mbr_id=ns.src_mbr_id
  inner join COMPACT.DIM_MEMBER m on  ns.src_mbr_id=m.memb_dim_id  and NS.DOB=indv.BTH_DT  and m.social_sec_no=indv.ssn and upper(indv.fst_nm)=upper(NS.memb_first_name)
      and upper(indv.lst_nm)=upper(NS.memb_last_name)
  left outer join
  ECT_prd_INDIVIDUAL_DB.FOUNDATION.DW_INDV_TEL_NBR TEL on   UPPER(TEL.DW_SYS_REF_CD)='IMDM' AND UPPER(TEL.DW_SRC_REC_STS_CD)='TRUE' and indv.indv_id=TEL.INDV_ID and
    TEL.CMNCT_TYP_DESC in ('Home Telephone','Mobile Telephone','Unknown Telephone')
    and TEL.SRC_SYS_CD='IMDM' and UPPER(TEL.ACTV)='TRUE'
    ));


   update NORTHSTAR.NS_MEMBER_DATATABLE  ns set ns.INDV_ID=imdm.INDV_ID1,ns.IMDM_HOME_PHONE_NUMBER=imdm.IMDM_HOME_PHONE,ns.IMDM_CELL_PHONE_NUMBER=imdm.IMDM_CELL_PHONE,ns.IMDM_UNKNOWN_PHONE_NUMBER=imdm.IMDM_UNKNOWN_PHONE  from
   northstar.ns_imdm imdm where imdm.src_mbr_id=ns.memb_dim_id;

   create or replace temporary table northstar.ns_imdm_missing as ( select memb_dim_id src_mbr_id,bene_first_name,bene_last_name,bene_DOB,SOCIAL_SEC_NO,SEQ_MEMB_ID,MEDICAID_NO
                                                                   from NORTHSTAR.NS_MEMBER_DATATABLE where indv_id is null );



create or replace temporary table northstar.ns_imdm as(select distinct final_indv_id indv_id1,src_mbr_id,MAX(IMDM_HOME_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_HOME_PHONE,
                                                       MAX(IMDM_CELL_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_CELL_PHONE,MAX(IMDM_UNKNOWN_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_UNKNOWN_PHONE
from (
select distinct indv_key.INDV_KEY_VAL,indv.INDV_ID,ns.src_mbr_id,NS.SEQ_MEMB_ID , NS.DOB, indv.BTH_DT,upper(indv.fst_nm)fst_nm,ns.memb_first_name,upper(indv.lst_nm) lst_nm,ns.memb_last_name,
indv.ssn,m.social_sec_no,case when TEL.CMNCT_TYP_DESC in ('Home Telephone') then TEL_NBR end IMDM_HOME_PHONE1,case when TEL.CMNCT_TYP_DESC in ('Mobile Telephone') then TEL_NBR end IMDM_CELL_PHONE1,
  case when TEL.CMNCT_TYP_DESC in ('Unknown Telephone') then first_value(TEL_NBR) over (partition by indv.indv_id order by TEL.LST_UPDT_SRC_SYS_TMSTMP desc) end IMDM_UNKNOWN_PHONE1,
  HOME_PHONE_NUMBER,CELL_PHONE_NUMBER,RES_PARTY_EMAIL,
     INDV_KEY.DW_SYS_REF_CD,first_value(indv.indv_id) over (partition by indv_key.INDV_KEY_VAL order by  indv.PERS_NM_LST_UPDT_SRC_SYS_TMSTMP desc) final_indv_id
  from  ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV_KEY indv_key inner join ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV indv
  on  INDV.INDV_ID=INDV_KEY.INDV_ID  and INDV_KEY_TYP_REF_DESC='medicaidRecipientNumber'
 AND INDV_KEY.ACTV='true' and  INDV_KEY.DW_SYS_REF_CD='IMDM' and indv.NM_TYP_DESC='Preferred' and INDV.SRC_SYS_CD='IMDM'
   and INDV.ACTV='true' and  UPPER(INDV.DW_SRC_REC_STS_CD)='TRUE'
  inner join NORTHSTAR.NS_MEMBER NS on ns.medicaid_no=indv_key.INDV_KEY_VAL
  inner join northstar.ns_imdm_missing ns1 on ns1.src_mbr_id=ns.src_mbr_id
  inner join COMPACT.DIM_MEMBER m on  ns.src_mbr_id=m.memb_dim_id    and m.social_sec_no=indv.ssn and upper(indv.fst_nm)=upper(NS.memb_first_name)
      and upper(indv.lst_nm)=upper(NS.memb_last_name)
  left outer join
  ECT_prd_INDIVIDUAL_DB.FOUNDATION.DW_INDV_TEL_NBR TEL on   UPPER(TEL.DW_SYS_REF_CD)='IMDM' AND UPPER(TEL.DW_SRC_REC_STS_CD)='TRUE' and indv.indv_id=TEL.INDV_ID and
    TEL.CMNCT_TYP_DESC in ('Home Telephone','Mobile Telephone','Unknown Telephone')
    and TEL.SRC_SYS_CD='IMDM' and UPPER(TEL.ACTV)='TRUE'
    ));


      update NORTHSTAR.NS_MEMBER_DATATABLE  ns set ns.INDV_ID=imdm.INDV_ID1,ns.IMDM_HOME_PHONE_NUMBER=imdm.IMDM_HOME_PHONE,ns.IMDM_CELL_PHONE_NUMBER=imdm.IMDM_CELL_PHONE,ns.IMDM_UNKNOWN_PHONE_NUMBER=imdm.IMDM_UNKNOWN_PHONE  from
   northstar.ns_imdm imdm where imdm.src_mbr_id=ns.memb_dim_id and ns.indv_id is null;

   create or replace temporary table northstar.ns_imdm_missing as ( select memb_dim_id src_mbr_id,bene_first_name,bene_last_name,bene_DOB,SOCIAL_SEC_NO,SEQ_MEMB_ID,MEDICAID_NO
                                                                   from NORTHSTAR.NS_MEMBER_DATATABLE where indv_id is null );


create or replace temporary table northstar.ns_imdm as(select distinct final_indv_id indv_id1,src_mbr_id,MAX(IMDM_HOME_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_HOME_PHONE,
                                                       MAX(IMDM_CELL_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_CELL_PHONE,MAX(IMDM_UNKNOWN_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_UNKNOWN_PHONE
from (
select distinct indv_key.INDV_KEY_VAL,indv.INDV_ID,ns.src_mbr_id,NS.SEQ_MEMB_ID , NS.DOB, indv.BTH_DT,upper(indv.fst_nm)fst_nm,ns.memb_first_name,upper(indv.lst_nm) lst_nm,ns.memb_last_name,
indv.ssn,m.social_sec_no,case when TEL.CMNCT_TYP_DESC in ('Home Telephone') then TEL_NBR end IMDM_HOME_PHONE1,case when TEL.CMNCT_TYP_DESC in ('Mobile Telephone') then TEL_NBR end IMDM_CELL_PHONE1,
  case when TEL.CMNCT_TYP_DESC in ('Unknown Telephone') then first_value(TEL_NBR) over (partition by indv.indv_id order by TEL.LST_UPDT_SRC_SYS_TMSTMP desc) end IMDM_UNKNOWN_PHONE1,
  HOME_PHONE_NUMBER,CELL_PHONE_NUMBER,RES_PARTY_EMAIL,
     INDV_KEY.DW_SYS_REF_CD,first_value(indv.indv_id) over (partition by indv_key.INDV_KEY_VAL order by  indv.PERS_NM_LST_UPDT_SRC_SYS_TMSTMP desc) final_indv_id
  from  ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV_KEY indv_key inner join ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV indv
  on  INDV.INDV_ID=INDV_KEY.INDV_ID  and INDV_KEY_TYP_REF_DESC='medicaidRecipientNumber'
 AND INDV_KEY.ACTV='true' and  INDV_KEY.DW_SYS_REF_CD='IMDM' and indv.NM_TYP_DESC='Preferred' and INDV.SRC_SYS_CD='IMDM'
   and INDV.ACTV='true' and  UPPER(INDV.DW_SRC_REC_STS_CD)='TRUE'
  inner join NORTHSTAR.NS_MEMBER NS on ns.medicaid_no=indv_key.INDV_KEY_VAL
  inner join northstar.ns_imdm_missing ns1 on ns1.src_mbr_id=ns.src_mbr_id
  inner join COMPACT.DIM_MEMBER m on  ns.src_mbr_id=m.memb_dim_id    and upper(indv.fst_nm)=upper(NS.memb_first_name)
      and upper(indv.lst_nm)=upper(NS.memb_last_name)
  left outer join
  ECT_prd_INDIVIDUAL_DB.FOUNDATION.DW_INDV_TEL_NBR TEL on   UPPER(TEL.DW_SYS_REF_CD)='IMDM' AND UPPER(TEL.DW_SRC_REC_STS_CD)='TRUE' and indv.indv_id=TEL.INDV_ID and
    TEL.CMNCT_TYP_DESC in ('Home Telephone','Mobile Telephone','Unknown Telephone')
    and TEL.SRC_SYS_CD='IMDM' and UPPER(TEL.ACTV)='TRUE'
    ));

  update NORTHSTAR.NS_MEMBER_DATATABLE  ns set ns.INDV_ID=imdm.INDV_ID1,ns.IMDM_HOME_PHONE_NUMBER=imdm.IMDM_HOME_PHONE,ns.IMDM_CELL_PHONE_NUMBER=imdm.IMDM_CELL_PHONE,ns.IMDM_UNKNOWN_PHONE_NUMBER=imdm.IMDM_UNKNOWN_PHONE  from
   northstar.ns_imdm imdm where imdm.src_mbr_id=ns.memb_dim_id and ns.indv_id is null;

   create or replace temporary table northstar.ns_imdm_missing as ( select memb_dim_id src_mbr_id,bene_first_name,bene_last_name,bene_DOB,SOCIAL_SEC_NO,SEQ_MEMB_ID,MEDICAID_NO
                                                                   from NORTHSTAR.NS_MEMBER_DATATABLE where indv_id is null );



   create or replace temporary table northstar.ns_imdm as(select distinct final_indv_id indv_id1,src_mbr_id,MAX(IMDM_HOME_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_HOME_PHONE,
                                                       MAX(IMDM_CELL_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_CELL_PHONE,MAX(IMDM_UNKNOWN_PHONE1) over (partition by src_mbr_id order by src_mbr_id) IMDM_UNKNOWN_PHONE
from (
select distinct indv_key.INDV_KEY_VAL,indv.INDV_ID,ns.src_mbr_id,NS.SEQ_MEMB_ID , NS.DOB, indv.BTH_DT,upper(indv.fst_nm)fst_nm,ns.memb_first_name,upper(indv.lst_nm) lst_nm,ns.memb_last_name,
indv.ssn,m.social_sec_no,case when TEL.CMNCT_TYP_DESC in ('Home Telephone') then TEL_NBR end IMDM_HOME_PHONE1,case when TEL.CMNCT_TYP_DESC in ('Mobile Telephone') then TEL_NBR end IMDM_CELL_PHONE1,
  case when TEL.CMNCT_TYP_DESC in ('Unknown Telephone') then first_value(TEL_NBR) over (partition by indv.indv_id order by TEL.LST_UPDT_SRC_SYS_TMSTMP desc) end IMDM_UNKNOWN_PHONE1,
  HOME_PHONE_NUMBER,CELL_PHONE_NUMBER,RES_PARTY_EMAIL,
     INDV_KEY.DW_SYS_REF_CD,first_value(indv.indv_id) over (partition by indv_key.INDV_KEY_VAL order by  indv.PERS_NM_LST_UPDT_SRC_SYS_TMSTMP desc) final_indv_id
  from  ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV_KEY indv_key inner join ECT_PRD_INDIVIDUAL_DB.FOUNDATION.DW_INDV indv
  on  INDV.INDV_ID=INDV_KEY.INDV_ID  and INDV_KEY_TYP_REF_DESC='medicaidRecipientNumber'
 AND INDV_KEY.ACTV='true' and  INDV_KEY.DW_SYS_REF_CD='IMDM' and indv.NM_TYP_DESC='Preferred' and INDV.SRC_SYS_CD='IMDM'
   and INDV.ACTV='true' and  UPPER(INDV.DW_SRC_REC_STS_CD)='TRUE'
  inner join NORTHSTAR.NS_MEMBER NS on ns.medicaid_no=indv_key.INDV_KEY_VAL
  inner join northstar.ns_imdm_missing ns1 on ns1.src_mbr_id=ns.src_mbr_id
  inner join COMPACT.DIM_MEMBER m on  ns.src_mbr_id=m.memb_dim_id     and substring(upper(indv.fst_nm),1,2)=substring(upper(NS.memb_first_name),1,2) --and m.social_sec_no=indv.ssn
and substring(upper(indv.lst_nm),1,2)=substring(upper(NS.memb_last_name),1,2)
  left outer join
  ECT_prd_INDIVIDUAL_DB.FOUNDATION.DW_INDV_TEL_NBR TEL on   UPPER(TEL.DW_SYS_REF_CD)='IMDM' AND UPPER(TEL.DW_SRC_REC_STS_CD)='TRUE' and indv.indv_id=TEL.INDV_ID and
    TEL.CMNCT_TYP_DESC in ('Home Telephone','Mobile Telephone','Unknown Telephone')
    and TEL.SRC_SYS_CD='IMDM' and UPPER(TEL.ACTV)='TRUE'
    ));


 update NORTHSTAR.NS_MEMBER_DATATABLE  ns set ns.INDV_ID=imdm.INDV_ID1,ns.IMDM_HOME_PHONE_NUMBER=imdm.IMDM_HOME_PHONE,ns.IMDM_CELL_PHONE_NUMBER=imdm.IMDM_CELL_PHONE,ns.IMDM_UNKNOWN_PHONE_NUMBER=imdm.IMDM_UNKNOWN_PHONE  from
   northstar.ns_imdm imdm where imdm.src_mbr_id=ns.memb_dim_id and ns.indv_id is null;



   update NORTHSTAR.NS_MEMBER_DATATABLE ns set ns.Altruista_id=ns1.Altruista_id from NORTHSTAR.NS_PROGRAM_PRIMARY_INSERT ns1  where ns1.src_mbr_id=ns.memb_dim_id;

   update NORTHSTAR.NS_MEMBER_DATATABLE ns set ns.AE_PROGRAM_STATUS=ns1.PROGRAM_STATUS,ns.AE_PROGRAM_STATUS_DESCRIPTION=ns1.PROGRAM_STATUS_DESC,ns.PROGRAM_NAME=ns1.PROGRAM_NAME,ns.PROGRAM_REFERRAL_SOURCE=ns1.PROGRAM_REFERRAL_SOURCE  from NORTHSTAR.NS_PROGRAM_PRIMARY_INSERT ns1  where ns1.src_mbr_id=ns.memb_dim_id;


result:='NS members loaded to data table successfully';
 return result;
 end;
 ';