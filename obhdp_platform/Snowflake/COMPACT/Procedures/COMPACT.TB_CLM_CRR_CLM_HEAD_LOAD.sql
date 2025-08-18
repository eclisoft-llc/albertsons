CREATE OR REPLACE PROCEDURE COMPACT.TB_CLM_CRR_CLM_HEAD_LOAD("STARTDATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '

      declare result varchar;
      load_strt_ts timestamp;
      load_end_ts timestamp;
      ins_cnt number;
      updt_cnt number;
      unique_id varchar;
      begin
       SELECT UUID_STRING() into unique_id;
       SELECT CURRENT_TIMESTAMP() INTO load_strt_ts ;


CREATE OR REPLACE TEMPORARY TABLE TEMP_AMT AS
(select h.claimid as claimid ,sum(h.billedandtaxamt) as CLM_TOT_AMT_SBMT_AMT, sum(h.copayreductamt) as CLM_TOT_COPAY_COINS_AMT,
sum(h.deductiblereductamt) as CLM_TOT_DED_AMT
from
COMPACT.CRR_CLAIMLINEADJUDICATION c ,
COMPACT.crr_claBenefit h, COMPACT.crr_benefitplan i
where
TRIM(h.planid) = TRIM(i.planid)
and TRIM(i.benefittype) = ''MD''
and TRIM(i.benplanstatus) = ''A''
and TRIM(h.is_active) = ''Y''
and TRIM(i.is_active) = ''Y''
and TRIM(c.is_active) = ''Y''
and c.claimlineadjudicationid = h.claimlineadjudicationid
and TRIM(c.cdc_flag) <> ''D'' and TRIM(c.is_active) = ''Y'' and TRIM(c.clastatus) in (''A'', ''D'') and c.benefitclaind = 1
group by h.claimid);
 
 
                                              
CREATE OR REPLACE TEMPORARY TABLE TEMP_ICD_DIAG_TYP_CD AS
(SELECT claimid,count(distinct diagcodetype) as CNT ,max(diagcodetype) as maxvalue, min(diagcodetype) as minvalue
FROM COMPACT.CRR_CLAIMDIAGNOSIS
where TRIM(is_active) = ''Y''
group by claimid);

CREATE OR REPLACE TEMPORARY TABLE TEMP_CLABENEFITCODE AS
(select c.claimID , count(distinct benefittierparstatustype) AS CNT
from COMPACT.CRR_CLAIMLINEADJUDICATION c
inner join COMPACT.CRR_CLABENEFITCODE n on c.claimlineadjudicationid = n.claimlineadjudicationid
where TRIM(c.cdc_flag) <> ''D'' and TRIM(c.is_active) = ''Y'' 
 and TRIM(c.clastatus) in (''A'', ''D'') and c.benefitclaind = 1 and TRIM(n.is_active) = ''Y''
group by c.claimID);
 
 
CREATE OR REPLACE TEMPORARY TABLE TEMP_CRR_APGENERATIONRESPONSE AS
(select * from 
(SELECT APTRANSACTIONID,PAYMENTDATE,OFFSETIND,APGENERATIONRESPONSEID,BTCH_LOAD_DT, RANK() OVER (PARTITION BY B.APTRANSACTIONID ORDER BY OFFSETIND DESC,APGENERATIONRESPONSEID DESC ) RNK 
FROM COMPACT.CRR_APGENERATIONRESPONSE B WHERE trim(B.IS_ACTIVE) = ''Y'' AND TRIM(B.CDC_FLAG) <> ''D'' )Z WHERE Z.RNK = 1 );
 
CREATE OR REPLACE TEMPORARY TABLE TEMP_CRR_CLM_HDR1 AS
(select distinct
b.BTCH_LOAD_DT as BTCH_LOAD_DT,
trim(b.CLAIMID) as CLM_AUD_NBR,
(case when trim(b.claimtype)=''3'' then ''2'' when trim(b.claimtype)=''2'' then ''1'' else null end) as CLM_FM_TYP_CD,
--(case when f.OFFSETIND=1 then ''adj'' else ''orig'' end) as CLM_TYP_CD,
d.CLAIMSTATUS as CLM_STS_CD,
d.CLAIMEVENTID as CLAIMEVENTID,
d.ORIGINALRECEIPTDATE as CLM_ENT_DT,
d.adjudicationdate as CLM_ADJD_DT,
b.DATEOFSERVICESTART as CLM_FST_SRVC_DT,
b.DATEOFSERVICEEND as CLM_LST_SRVC_DT,
--f.paymentDate as CLM_PD_DT,
d.CIRRUSRECEIPTDATE as CLM_RECV_DT,
g.statementfromdate as CLM_STMT_FROM_DT,
g.statementthrudate as CLM_STMT_TO_DT,
--f.paymentDate as CHK_DT,
(Case when b.TOTALCOBPAIDAMT is null then 0.00 Else b.TOTALCOBPAIDAMT End) as CLM_TOT_COB_SV_AMT,
(Case when b.TOTALBILLEDAMT is null then 0.00 Else b.TOTALBILLEDAMT End) as CLM_SBMT_TOT_CHRG_AMT,
(Case when b.TOTALPAIDAMT is null then 0.00 Else b.TOTALPAIDAMT End) as CLM_TOT_PD_AMT,
(Case when b.TOTALMEMRESPAMT is null then 0.00 Else b.TOTALMEMRESPAMT End) as CLM_TOT_MBR_RESP_AMT,
(Case when temp.CLM_TOT_AMT_SBMT_AMT is null then 0.00 Else temp.CLM_TOT_AMT_SBMT_AMT End) as CLM_TOT_AMT_SBMT_AMT,
(Case when temp.CLM_TOT_COPAY_COINS_AMT is null then 0.00 Else temp.CLM_TOT_COPAY_COINS_AMT End) as CLM_TOT_COPAY_COINS_AMT,
(Case when temp.CLM_TOT_DED_AMT is null then 0.00 Else temp.CLM_TOT_DED_AMT End) as CLM_TOT_DED_AMT,
b.CLAIMSUBMISSIONTYPE as CLM_SBMT_SRC_CD,
r.REFERENCEDESC as CLM_SBMT_SRC_DESC,
TRIM(MBR.GDR_CD) as CLM_MBR_GDR_CD,
TRIM(MBR.REL_CD) as CLM_MBR_REL_CD,
l.MEMBERID as CLM_MBR_SRC_MBR_SYS_ID,
l.SUBSCRIBERID as CLM_MBR_SRC_SBSCR_ID,
trim(l.MEMGROUPID) as CLM_MBR_SRC_SGRP_ID,
trim(l.MEMGROUPCONTRACTID) as CLM_SRC_CUST_CONTR_ID,
--e.payeeEntityType as PAYEEENTITYTYPE,
--e.CHANGEDATETIME as APCHANGEDATETIME,
--e.UPDATEVERSION,
--e.CREATEDATETIME as APCREATEDATETIME,
--f.paymentDate as PAYMENTDATE,
trim(g.patientstatuscode) as PATIENTSTATUSCODE,
trim(k.referencedesc) as REFERENCEDESC,
replace(coalesce(trim(m.diagnosiscode), ''''), ''.'','''') as DIAGNOSISCODE,
m.diagseqnum as DIAGSEQNUM,
g.admissiondate as ADMIT_DT,
trim(g.ADMISSIONTYPECODE) as ADMIT_TYP_CD,
trim(g.ADMISSIONSOURCECODE) as ADMIT_SRC_CD,
b.PRIORAUTHNUM as CLM_SRVC_AUTH_NBR,
(case when  n1.CNT=1 and n.benefittierparstatustype=''1'' then ''Y'' when n1.CNT=1 and n.benefittierparstatustype=''2'' then ''N'' else ''U'' end) as IN_NTWK_PLN_IND,
n.claimlineadjudicationid,
g.TYPEOFBILL as TYPEOFBILL,
o.drgcode as ADJD_DIAG_REL_GRP_CD,
g.submitteddrgcode as SBMT_DIAG_REL_GRP_CD,
o.drgversion as GRPR_SFTWE_VER_NUM,
o.CLAIMPRICINGOUTPUTHEADERID as CLAIMPRICINGOUTPUTHEADERID,
q.procedurecode as PROC_CD,
q.instprocseqnum as INSTPROCSEQNUM,
q.proccodetype as PROC_QUAL_TYP_CD,
(Case when MBR.REL_CD =''18'' then ''SUB'' Else ''MEM'' End) as CLM_CNSM_ROLE_TYP_CD,
b.CREATEDATETIME as CREATEDATETIME,
b.CHANGEDATETIME as CHANGEDATETIME,
trim(b.CLAIMID) as CRR_CLM_HEAD_SYS_ID,
(CASE WHEN m.CNT=1 THEN m.diagcodetype
      when m.CNT>1 and (m.maxvalue=''0'' and  m.minvalue='''') then ''0''
      when m.CNT>1 and (m.maxvalue=''9'' and  m.minvalue=''0'') then ''UNKNOWN''
       when m.CNT>1 and (m.maxvalue=''9'' and  m.minvalue='''') then ''9''
       when m.diagcodetype is null and b.dateofserviceend < ''2015-10-01'' then ''9''
       when m.diagcodetype is null and b.dateofserviceend >= ''2015-10-01'' then ''0''
       else null end) as ICD_DIAG_TYP_CD,
       (case when ICD_DIAG_TYP_CD=''9'' and m.admittingdiagind=1 then ''BJ''
            when ICD_DIAG_TYP_CD=''9'' and m.admittingdiagind<>1 and m.principalDiagInd=1 then ''BK''   
            when ICD_DIAG_TYP_CD=''9'' and m.admittingdiagind<>1 and m.principalDiagInd=0 then ''BF''
            when ICD_DIAG_TYP_CD=''0'' and m.admittingdiagind=1 then ''ABJ''
            when ICD_DIAG_TYP_CD=''0'' and m.admittingdiagind<>1 and m.principalDiagInd=1 then ''ABK''   
            when ICD_DIAG_TYP_CD=''0'' and m.admittingdiagind<>1 and m.principalDiagInd=0 then ''ABF''
        ELSE '''' end) as DIAG_ROLE_TYP_CD,
m.principalDiagInd,
coalesce(trim(s.PROVIDERID),''0'')||''-''||coalesce(trim(s.PROVIDERORGAFFILIATIONID),''0'')||''-''||coalesce(trim(b.PROVIDERORGID),''0'')||''-''||coalesce(trim(u.PROVIDERORGLOCATIONID),''0'') AS CLM_PROV_SRC_PROV_ID,
addressentityidcode as CLM_PROV_ADR_TYPE_CD,
coalesce(trim(ce.INTERNALADJUSTREASONCODE),'''') as CLM_LVL_RSN_CD,
coalesce(trim(ij.IARCCODEDESC),'''') as CLM_LVL_RSN_CD_DESC,
case when bp.FUNDINGSOURCE = ''01'' Then ''F'' When  bp.FUNDINGSOURCE = ''02'' then ''A'' Else '''' end as CLM_SHR_ARNG_CD ,
coalesce(trim(bp.PLANTYPE),'''') as CLM_MHSA_PRDCT_CD,
coalesce(trim(ref.REFERENCEDESC),'''') as CLM_MHSA_PRDCT_DESC,
coalesce(trim(c.LINEOFBUSINESSID),'''') as CLM_MKT_SEG_CD,
coalesce(trim(c.CARRIERID),'''') as CLM_CO_CD,
--coalesce(trim(substr(addr.POSTALCODE,1,5)),'''')as CLM_MBR_MKT_DERIV_NBR,
RIGHT(sae.AFFILIATIONEXTERNALID ,2 ) as CLM_MBR_DEPN_NBR
FROM
--COMPACT.TB_CLM_CRR_BH_CLM_WRK a INNER JOIN
COMPACT.CRR_CLAIMHEADER b
--on a.clm_aud_nbr = trim(b.claimid)
inner join COMPACT.CRR_CLAIMLINEADJUDICATION c on TRIM(b.claimID) = TRIM(c.claimID)
inner join COMPACT.CRR_CLABENEFIT h  on c.claimlineadjudicationid = h.claimlineadjudicationid
left outer join COMPACT.crr_benefitplan bp on  TRIM(h.planid) = TRIM(bp.planid)
and trim(bp.benefittype) = ''MD'' and trim(bp.benplanstatus) = ''A'' and  trim(bp.is_active) = ''Y'' --join5
left outer join COMPACT.CRR_CLAIMEVENT d on TRIM(c.claimEventID) = TRIM(d.claimEventID)
 and TRIM(c.claimid) = TRIM(d.claimid) and  trim(d.is_active) = ''Y''
--left outer join COMPACT.CRR_APTRANSACTION e on TRIM(c.claimID) = TRIM(e.claimID)
-- and TRIM(c.claimEventID) = TRIM(e.claimEventID) and trim(e.is_active) = ''Y''
-- left outer join TEMP_CRR_APGENERATIONRESPONSE f on TRIM(e.apTransactionID) = TRIM(f.apTransactionID)
left outer join COMPACT.CRR_CLAIMHEADERINST g on TRIM(b.claimID) = TRIM(g.claimID)
 and trim(g.is_active) = ''Y'' and trim(b.claimType) = ''2''
left outer join TEMP_AMT temp on TRIM(temp.claimid) = TRIM(h.claimid)
left outer join COMPACT.CRR_CLAIMMEMBER j on TRIM(c.claimid) = TRIM(j.claimid) and trim(j.is_active) = ''Y''
left outer join COMPACT.CRR_REFERENCE k on trim(k.referencename) = ''patientStatusCode'' 
 and TRIM(g.patientstatuscode) = TRIM(k.referencecode) and trim(k.is_active)=''Y''
left outer join COMPACT.CRR_MEMBERBENEFIT l on  TRIM(h.memberbenefitid) = TRIM(l.memberbenefitid)
 and trim(l.is_active) = ''Y''
left outer join (select m1.claimid as claimid ,m1.diagcodetype as diagcodetype ,m1.admittingdiagind,m1.principalDiagInd,
m1.diagnosiscode,m1.diagseqnum,
m2.CNT as CNT,m2.maxvalue as maxvalue,
m2.minvalue as minvalue
from COMPACT.CRR_CLAIMDIAGNOSIS m1 inner join TEMP_ICD_DIAG_TYP_CD m2 on TRIM(m1.claimid) = TRIM(m2.claimid) 
and trim(m1.is_active) = ''Y'') m on TRIM(b.claimid)=TRIM(m.claimid)
left outer join COMPACT.CRR_CLABENEFITCODE n on TRIM(c.claimlineadjudicationid) = 
 TRIM(n.claimlineadjudicationid) and trim(n.is_active) = ''Y''
left outer join TEMP_CLABENEFITCODE n1 on TRIM(n1.claimid)=TRIM(c.claimid)
left outer join COMPACT.CRR_CLAIMPRICINGOUTPUTHEADER o on TRIM(g.claimid) = TRIM(o.claimid) and trim(o.is_active) = ''Y''
-- LEFT OUTER JOIN #DWSchema#.TB_CLM_CRR_CLM_HEAD p on trim(convert(varchar(20),b.CLAIMID)) = p.CLM_AUD_NBR and b.IS_ACTIVE=''Y''
left outer join COMPACT.CRR_CLAIMINSTPROC q on TRIM(g.claimid) = TRIM(q.claimid) and q.instProcSeqNum = 1 and trim(q.is_active) = ''Y''
left outer join COMPACT.CRR_REFERENCE r on trim(r.referencename) = ''claimSubmissionType'' and trim(b.claimsubmissiontype) = trim(r.referencecode) and trim(r.is_active)=''Y''
left outer join COMPACT.CRR_CLAIMPROVIDER s on TRIM(b.claimid) = TRIM(s.claimid)
 and TRIM(b.renderingclaimproviderid) = TRIM(s.claimproviderid) and trim(s.is_active)=''Y''
left outer join COMPACT.CRR_CLAIMPROVIDERADDRESS u on TRIM(b.claimid) = TRIM(u.claimid)
 and  TRIM(b.renderingclaimprovideraddressid) = TRIM(u.claimprovideraddressid) and trim(u.is_active)=''Y''
LEFT OUTER JOIN COMPACT.TB_MEM_MBR MBR on trim(l.MEMBERID) =
substring (substring(trim(mbr_src_key_id), CHARINDEX(''-'', (trim(mbr_src_key_id)))+1,len(trim(nvl(mbr_src_key_id,'''')))) ,1,CHARINDEX(''-'', substring(trim(mbr_src_key_id), CHARINDEX(''-'', (trim(mbr_src_key_id)))+1,len(trim(nvl(mbr_src_key_id,'''')))))-1)
and trim(MBR.REC_INPUT_SRC_CD)=''CRR'' and trim(MBR.REC_XTRCT_SRC_CD)=''CRR''
--usp
Left outer Join COMPACT.CRR_CLAIMEVENT ce  on TRIM(ce.CLAIMID) = TRIM(o.claimid) and  trim(ce.IS_ACTIVE) = ''Y''  -- join 18
Left outer Join COMPACT.CRR_INTERNALADJUSTREASONCODE ij  on  trim(ij.INTERNALADJUSTREASONCODE) =  trim(ce.INTERNALADJUSTREASONCODE)  AND trim(ij.IS_ACTIVE) = ''Y'' --join19
Left Outer Join COMPACT.CRR_SUBSAFFILIATIONEXTERNALID sae on trim(sae.MEMBERID)  = trim(MBR.MBR_SYS_ID)   And trim(sae.EXTERNALIDTYPE)  = ''SC'' and  trim(sae.IS_ACTIVE) = ''Y'' --join20
Left outer join COMPACT.CRR_REFERENCE ref on trim(ref.referencename) =  ''planType'' and trim(bp.PLANTYPE) = trim(ref.REFERENCECODE) and  trim(ref.IS_ACTIVE) = ''Y'' and  trim(bp.IS_ACTIVE) = ''Y'' --join21
--Left outer Join COMPACT.CRR_MEMGROUPADDRESS addr on l.MEMGROUPID = addr.MEMGROUPID  and  trim(l.IS_ACTIVE) = ''Y'' and  trim(addr.IS_ACTIVE) = ''Y'' --join22
WHERE
(b.BTCH_LOAD_DT>=DATE(''2023-03-01'') OR c.BTCH_LOAD_DT>=DATE(''2023-03-01'') 
 --or f.BTCH_LOAD_DT >=DATE(''2023-03-01'')
)
and TRIM(b.claimtype) in (''2'', ''3'')  and TRIM(b.is_active) = ''Y'' and TRIM(b.cdc_flag) <> ''D''  and TRIM(b.claimstatus) = ''A''
and TRIM(c.cdc_flag) <> ''D'' and TRIM(c.is_active) = ''Y'' and TRIM(c.clastatus) in (''A'', ''D'') and TRIM(c.benefitclaind) = 1 and TRIM(h.is_active) =''Y''
);



CREATE OR REPLACE TEMPORARY TABLE TEMP_CRR_CLM_HDR AS
(select c.*,
(case when f.OFFSETIND=1 then ''adj'' else ''orig'' end) as CLM_TYP_CD,
f.paymentDate as CLM_PD_DT,
f.paymentDate as CHK_DT,
e.payeeEntityType as PAYEEENTITYTYPE,
e.CHANGEDATETIME as APCHANGEDATETIME,
e.UPDATEVERSION,
e.CREATEDATETIME as APCREATEDATETIME,
f.paymentDate as PAYMENTDATE,
coalesce(trim(substr(addr.POSTALCODE,1,5)),'''')as CLM_MBR_MKT_DERIV_NBR
 from 
TEMP_CRR_CLM_HDR1 c
left outer join COMPACT.CRR_APTRANSACTION e on TRIM(c.CLM_AUD_NBR) = TRIM(e.claimID)
and TRIM(c.claimEventID) = TRIM(e.claimEventID) and trim(e.is_active) = ''Y''
left outer join TEMP_CRR_APGENERATIONRESPONSE f on TRIM(e.apTransactionID) = TRIM(f.apTransactionID) and f.BTCH_LOAD_DT >=DATE(''2023-03-01'')
Left outer Join COMPACT.CRR_MEMGROUPADDRESS addr on c.CLM_MBR_SRC_SGRP_ID = addr.MEMGROUPID  and  trim(addr.IS_ACTIVE) = ''Y''
) ;


 
 
 
CREATE OR REPLACE TABLE COMPACT.TB_CLM_CRR_CLM_HEAD AS
(SELECT
HDR.CLM_AUD_NBR as CRR_CLM_HEAD_SYS_ID,
HDR.CLM_AUD_NBR as CLM_AUD_NBR,
HDR.CLM_FM_TYP_CD as CLM_FM_TYP_CD,
HDR.CLM_TYP_CD as CLM_TYP_CD,
HDR.CLM_STS_CD as CLM_STS_CD,
HDR.CLM_ENT_DT as CLM_ENT_DT,
HDR.CLM_ADJD_DT as CLM_ADJD_DT,
HDR.CLM_FST_SRVC_DT AS CLM_FST_SRVC_DT,
HDR.CLM_LST_SRVC_DT AS CLM_LST_SRVC_DT,
HDR.CLM_PD_DT AS CLM_PD_DT,
HDR.CLM_RECV_DT AS CLM_RECV_DT,
HDR.CLM_STMT_FROM_DT AS CLM_STMT_FROM_DT,
HDR.CLM_STMT_TO_DT AS CLM_STMT_TO_DT,
HDR.CHK_DT AS CLM_CHK_DT,
''0.00'' AS CLM_TOT_AMT_DSCNT_AMT,
HDR.CLM_TOT_AMT_SBMT_AMT AS CLM_TOT_AMT_SBMT_AMT,
HDR.CLM_TOT_COB_SV_AMT AS CLM_TOT_COB_SV_AMT,
HDR.CLM_SBMT_TOT_CHRG_AMT AS CLM_SBMT_TOT_CHRG_AMT,
HDR.CLM_TOT_PD_AMT AS CLM_TOT_PD_AMT,
''0.00'' AS CLM_TOT_WTHLD_AMT,
HDR.CLM_TOT_COPAY_COINS_AMT AS CLM_TOT_COPAY_COINS_AMT,
HDR.CLM_TOT_DED_AMT AS CLM_TOT_DED_AMT,
HDR.CLM_TOT_MBR_RESP_AMT AS CLM_TOT_MBR_RESP_AMT,
HDR.CLM_SBMT_SRC_CD AS CLM_SBMT_SRC_CD,
HDR.CLM_SBMT_SRC_DESC AS CLM_SBMT_SRC_DESC,
HDR.CLM_MBR_GDR_CD AS CLM_MBR_GDR_CD,
HDR.CLM_MBR_REL_CD AS CLM_MBR_REL_CD,
nvl(HDR.CLM_MBR_DEPN_NBR,0) AS CLM_MBR_DEPN_NBR,
HDR.CLM_MBR_SRC_MBR_SYS_ID AS CLM_MBR_SRC_MBR_SYS_ID,
HDR.CLM_MBR_SRC_SBSCR_ID as CLM_MBR_SRC_SBSCR_ID,
HDR.CLM_MBR_SRC_SGRP_ID AS CLM_MBR_SRC_SGRP_ID,
HDR.CLM_SRC_CUST_CONTR_ID AS CLM_SRC_CUST_CONTR_ID,
CASE WHEN trim(HDR.PAYEEENTITYTYPE)=''7'' then ''F'' else ''V'' END AS CLM_PAYE_ASGN_TYP_CD,
HDR.CLM_PROV_SRC_PROV_ID AS CLM_PROV_SRC_PROV_ID,
HDR.PATIENTSTATUSCODE AS DSCHRG_STS_CD,
HDR.REFERENCEDESC AS DSCHRG_STS_DESC,
HDR.DIAGNOSISCODE AS CLM_PRI_DIAG_CD,
HDR.ADMIT_DT AS ADMIT_DT,
HDR.ADMIT_TYP_CD AS ADMIT_TYP_CD,
HDR.ADMIT_SRC_CD AS ADMIT_SRC_CD,
HDR.CLM_SRVC_AUTH_NBR AS CLM_SRVC_AUTH_NBR,
'''' AS CLM_AUTH_NUM_SUFX,
HDR.IN_NTWK_PLN_IND AS IN_NTWK_PLN_IND,
HDR.TYPEOFBILL AS ADJD_BIL_TYP_CD,
HDR.TYPEOFBILL AS SBMT_BIL_TYP_CD,
HDR.ADJD_DIAG_REL_GRP_CD AS ADJD_DIAG_REL_GRP_CD,
HDR.SBMT_DIAG_REL_GRP_CD AS SBMT_DIAG_REL_GRP_CD,
'''' AS DIAG_REL_GRP_TYP_CD,
HDR.GRPR_SFTWE_VER_NUM AS GRPR_SFTWE_VER_NUM,
HDR.ICD_DIAG_TYP_CD AS ICD_DIAG_TYP_CD,
HDR.DIAG_ROLE_TYP_CD AS DIAG_ROLE_TYP_CD,
HDR.PROC_CD AS PROC_CD,
CASE WHEN HDR.INSTPROCSEQNUM=''1'' then ''Y'' else ''N'' END AS PRIN_PROC_IND,
HDR.INSTPROCSEQNUM AS PROC_PSTN_NBR,
HDR.PROC_QUAL_TYP_CD AS PROC_QUAL_TYP_CD,
HDR.CLM_CNSM_ROLE_TYP_CD AS CLM_CNSM_ROLE_TYP_CD,
HDR.CLM_PROV_ADR_TYPE_CD AS CLM_PROV_ADR_TYP_CD,
HDR.CREATEDATETIME AS SRC_REC_CRT_DTTM,
HDR.CHANGEDATETIME AS SRC_REC_UPDT_DTTM,
CURRENT_DATE() AS REC_CRT_DTTM,
CURRENT_DATE() AS REC_UPDT_DTTM,
''CRR'' AS REC_XTRCT_SRC_CD,
''CRR'' AS REC_INPUT_SRC_CD,
HDR.BTCH_LOAD_DT AS BTCH_LOAD_DT,
HDR.CLM_LVL_RSN_CD AS CLM_LVL_RSN_CD,
HDR.CLM_LVL_RSN_CD_DESC AS CLM_LVL_RSN_CD_DESC,
HDR.CLM_SHR_ARNG_CD AS CLM_CES_FINC_ARNG_CD,
HDR.CLM_MHSA_PRDCT_CD AS CLM_MHSA_PRDCT_CD,
HDR.CLM_MHSA_PRDCT_DESC AS CLM_MHSA_PRDCT_DESC,
HDR.CLM_MKT_SEG_CD AS CLM_MKT_SEG_CD,
HDR.CLM_CO_CD AS CLM_CO_CD,
HDR.CLM_MBR_MKT_DERIV_NBR AS CLM_MBR_MKT_DERIV_NBR
FROM TEMP_CRR_CLM_HDR HDR qualify row_number() OVER (PARTITION BY HDR.CLM_AUD_NBR 
ORDER BY HDR.CLM_AUD_NBR asc,HDR.CLM_TYP_CD asc nulls first, HDR.CLAIMEVENTID desc nulls last, 
HDR.CLAIMPRICINGOUTPUTHEADERID desc nulls last, HDR.PRINCIPALDIAGIND desc nulls last, HDR.DIAGSEQNUM asc nulls last,
HDR.CLAIMLINEADJUDICATIONID desc nulls last, HDR.APCHANGEDATETIME desc nulls last, HDR.UPDATEVERSION desc nulls last,
HDR.APCREATEDATETIME desc nulls last, HDR.PAYMENTDATE asc nulls last ) =1 );
 
 
SELECT COUNT(1) INTO ins_cnt FROM COMPACT.TB_CLM_CRR_CLM_HEAD;

SELECT CURRENT_TIMESTAMP() INTO load_end_ts ;

 INSERT INTO ETL.DBA_PROCESS_LOG VALUES
 (:unique_id,''TB_CLM_CRR_CLM_HEAD'',''TB_CLM_CRR_CLM_HEAD'',''LOAD'',0,0,:load_strt_ts,:load_end_ts,
 CURRENT_TIMESTAMP,:ins_cnt,0,0) ;
 
 result:=''TB_CLM_CRR_CLM_HEAD refresh completed successfully'';
 return result;
 end;
 
 ';