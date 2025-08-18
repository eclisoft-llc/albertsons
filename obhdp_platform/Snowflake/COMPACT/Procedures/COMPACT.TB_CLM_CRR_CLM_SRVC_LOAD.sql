CREATE OR REPLACE PROCEDURE COMPACT.TB_CLM_CRR_CLM_SRVC_LOAD("STARTDATE" VARCHAR(16777216))
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

CREATE OR REPLACE TEMPORARY TABLE TEMP_SRC_QUERY1 AS
SELECT   c.CLAIMID AS UNIQ_CLM_AUD_NBR,
         c.CLAIMLINENUM AS CLM_AUD_LN_NBR,
        -- coalesce(p.CRR_CLM_SRVC_SYS_ID,-1) as CRR_CLM_SRVC_SYS_ID,
		TRIM(c.CLAIMID)||''-''||RIGHT(''000''||TRIM(c.CLAIMLINENUM),3) as CRR_CLM_SRVC_SYS_ID,
         c.CLAIMID as CLAIMID,
         g.CLAIMID as CLAIMID_G,
         c.BTCH_LOAD_DT as BTCH_LOAD_DT,
         b.BTCH_LOAD_DT AS BTCHLOADDT,
         (case when trim(b.CLM_TYP_CD) = ''adj'' then ''A'' else '''' end) as CLM_SRVC_ADJ_CD,
         c.CLASTATUS as CLM_SRVC_CLM_STS_CD,
         (case when z.claimtype = 2 then f.DATEOFSERVICEFROM when z.claimtype = 3 then g.DATEOFSERVICEFROM else '''' end) as SRVC_STRT_DT,
        (case when z.claimtype = 2 then f.DATEOFSERVICETHRU when z.claimtype = 3 then g.DATEOFSERVICETHRU else '''' end) as SRVC_END_DT,
         i.PAYMENTDATE as CLM_SRVC_PD_DT,
         b.CLM_CHK_DT as CLM_SRVC_PST_DT,
         --l.ADJUDICATIONDATE as CLM_SRVC_ADJD_DT,
         j.ALLOWEDAMT,
         j.COBADJUSTMENTAMT,
         j.MEMCOINSURANCEREDUCTAMT,
         j.COPAYREDUCTAMT,
         j.DEDUCTIBLEREDUCTAMT,
         j.PROMPTPAYDISCOUNTREDUCTAMT,
         j.BILLEDANDTAXAMT,

         c.FINALMEMRESPAMT,
         c.FINALNETPAYAMT,
         (case when z.claimtype=2 then f.SERVICEUNITS when z.claimtype=3 then g.SERVICEUNITS else '''' end) as CLM_SRVC_UNIT_CNT,
         (case when z.claimtype=2 then f.SERVICEUNITS when z.claimtype=3 then g.SERVICEUNITS else '''' end) as CLM_SRVC_UNIT_DERIV_CNT,
     
         (case when z.claimtype=2 then f.MODIFIER1 when z.claimtype=3 then g.MODIFIER1 else '''' end) as CLM_SRVC_PROC_MOD_CD,
         (case when z.claimtype=2 then f.MODIFIER2 when z.claimtype=3 then g.MODIFIER2 else '''' end) as CLM_SRVC_PROC_MOD_2_CD,
         (case when z.claimtype=2 then f.MODIFIER3 when z.claimtype=3 then g.MODIFIER3 else '''' end) as CLM_SRVC_PROC_MOD_3_CD,
         (case when z.claimtype=2 then f.MODIFIER4 when z.claimtype=3 then g.MODIFIER4 else '''' end) as CLM_SRVC_PROC_MOD_4_CD,
         c.LINEOFBUSINESSID,
         b.CLM_MBR_SRC_MBR_SYS_ID,
         i.CHECKNUM,
         i.CHANGEDATETIME as CHANGEDATETIME,
         h.UPDATEVERSION,
         h.CREATEDATETIME as CREATEDATETIME,
         h.PAYEEENTITYTYPE,
         (case when z.claimtype=2 then f.PROCEDURECODE when z.claimtype=3 then g.PROCEDURECODE else '''' end) as CLM_SRVC_PROC_CD,
         --(case when k.DIAGCODETYPE=9 then ''ICD9'' when k.DIAGCODETYPE=0 then ''ICD10'' else '''' end) as ICD_DIAG_TYP_CD,
        c.CREATEDATETIME as SRC_REC_CRT_DTTM,
         c.CHANGEDATETIME as SRC_REC_UPDT_DTTM,
         b.CRR_CLM_HEAD_SYS_ID,
         b.CLM_TYP_CD,
         b.CLM_FM_TYP_CD,
         trim(coalesce(g.POSCODE,'''')) AS POSCODE,
         trim(coalesce(f.REVENUECODE,'''')) AS REVENUECODE,
		 case when c.CARRIERID in (''5'',''6'',''7'',''8'',''1088476'') Then ''OHP_BEHAV_HLTH_DOFR_SCH''
	    when c.CARRIERID in (9,17,26,29,36,1190006,1048481,31,39,1,2,3,4,10,11,13,14,15,16,118,19,20,21,22,23,25,28,30,32,33,34,35,37,38,1048482,1179853) Then ''1028481'' else '''' end as DOFRSCHEDID,
      (Case When DOFRSCHEDID = ''OHP_BEHAV_HLTH_DOFR_SCH'' Then DOFRSCHEDID Else '''' End) as SRC_FINC_LIAB_CD,
      (Case When DOFRSCHEDID in (''OHP_BEHAV_HLTH_DOFR_SCH'' , ''1028481'' ) Then ''True'' Else ''False'' End) as DERIV_OBH_FINC_LIAB,
	  z.dateofservicestart,
      z.PROVIDERORGID,
      d.SVCLINESTATUS AS SVCLINESTATUS_D,
      d.USERCLAIMCANCELCODE AS USERCLAIMCANCELCODE_D,
      f.USERCLAIMCANCELCODE AS USERCLAIMCANCELCODE_F,
      g.USERCLAIMCANCELCODE AS USERCLAIMCANCELCODE_G,
      f.SVCLINESTATUS AS SVCLINESTATUS_F,
      c.claimEventID,
      z.claimid AS claimid_Z,
      f.renderingclaimproviderid,
      f.renderingclaimprovideraddressid,
      g.renderingclaimprovideraddressid AS renderingclaimprovideraddressid_G,
      g.renderingclaimproviderid AS renderingclaimproviderid_G,
      c.CLAIMLINENUM,
      c.claimsubscriberid,
      Z.MEMBERID,
      b.CLM_SRVC_AUTH_NBR,
	           n.claimLineAdjudicationID,
         n.PRICEDAMT,
         n.NONCOVEREDAMT,
         k0.CDC_FLAG,
         (case WHEN k0.CDC_FLAG!=''D'' THEN CASE when k0.DIAGCODETYPE=9 then ''ICD9'' when k0.DIAGCODETYPE=0 then ''ICD10'' else '''' end ELSE '''' END) as ICD_DIAG_TYP_CD,
         		 replace(coalesce(k0.DIAGNOSISCODE, ''''), ''.'','''') AS DIAGNOSISCODE_KO,
         CASE WHEN (k0.admittingdiagind = 1 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_ADMIT_CD,
         CASE WHEN (k0.diagseqnum = 1 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_1_CD,
         CASE WHEN (k0.diagseqnum = 2 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_2_CD,
         CASE WHEN (k0.diagseqnum = 3 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_3_CD,
         CASE WHEN (k0.diagseqnum = 4 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_4_CD,
         CASE WHEN (k0.diagseqnum = 5 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_5_CD,
         CASE WHEN (k0.diagseqnum = 6 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_6_CD,
         CASE WHEN (k0.diagseqnum = 7 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_7_CD,
         CASE WHEN (k0.diagseqnum = 8 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_8_CD,
         CASE WHEN (k0.diagseqnum = 9 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_9_CD,
         CASE WHEN (k0.diagseqnum = 10 ) THEN DIAGNOSISCODE_KO ELSE '''' END as CLM_SRVC_DIAG_10_CD
FROM    -- COMPACT.TB_CLM_CRR_BH_CLM_WRK a 
         -- inner join 
		 COMPACT.TB_CLM_CRR_CLM_HEAD b -- on a.CLM_AUD_NBR=b.CLM_AUD_NBR 
         inner join COMPACT.CRR_CLAIMHEADER z on trim(b.CLM_AUD_NBR) = TRIM(z.claimid) 
         inner join COMPACT.CRR_CLAIMLINEADJUDICATION c on trim(z.claimid) = trim(c.claimid) 
         inner join COMPACT.CRR_CLABENEFIT j on trim(c.claimlineadjudicationid) = trim(j.claimlineadjudicationid) and trim(c.claimid)=trim(j.claimid) 
         left outer join COMPACT.CRR_CLAIMLINEPROF d on trim(c.claimid) = trim(d.claimid) and trim(c.CLAIMLINENUM) = trim(d.CLAIMLINENUM) and (d.is_active)=''Y'' and (d.CDC_FLAG) <> ''D'' 
         left outer join COMPACT.CRR_CLAIMLINEINST f on trim(c.claimid) = trim(f.claimid) and trim(c.CLAIMLINENUM) = trim(f.CLAIMLINENUM) and (f.is_active)=''Y'' and (f.CDC_FLAG) <> ''D'' 
         left outer join COMPACT.CRR_CLAIMLINEPROF g on trim(c.claimid) = trim(g.claimid) and trim(c.CLAIMLINENUM) = trim(g.CLAIMLINENUM) and (g.is_active)=''Y'' and (g.CDC_FLAG) <> ''D'' 
         left outer join COMPACT.CRR_APTRANSACTION h on trim(c.claimid) = trim(h.claimid) and trim(c.claimeventid) = trim(h.claimeventid) and (h.is_active)=''Y'' and (h.CDC_FLAG) <> ''D'' 
         left outer join COMPACT.CRR_APGENERATIONRESPONSE i on TRIM(h.apTransactionID) = TRIM(i.apTransactionID) and (i.is_active)=''Y'' and TRIM(i.CDC_FLAG) <> ''D''  AND (i.OFFSETIND) <> 1
		  left outer join COMPACT.CRR_CLABENEFITALLOWED n on c.claimLineAdjudicationID = n.claimLineAdjudicationID and n.is_active=''Y'' AND n.CDC_FLAG <> ''D'' 
		  left outer join COMPACT.CRR_CLAIMDIAGNOSIS k0 ON trim(c.claimid) = trim(k0.claimid) and (k0.is_active=''Y'')
		 WHERE    (--a.BTCH_LOAD_DT>=''2000-01-01''
       b.BTCH_LOAD_DT>=DATE(:STARTDATE)
     OR  c.BTCH_LOAD_DT>=DATE(:STARTDATE)
     OR  z.BTCH_LOAD_DT>=DATE(:STARTDATE)
     OR  j.BTCH_LOAD_DT>=DATE(:STARTDATE) OR h.BTCH_LOAD_DT>=DATE(:STARTDATE) OR i.BTCH_LOAD_DT>=DATE(:STARTDATE)  OR f.BTCH_LOAD_DT>=DATE(:STARTDATE))
AND	 TRIM(z.is_active)=''Y''
AND      z.claimtype in (2,3)
AND      TRIM(z.CDC_FLAG) <> ''D''
AND      TRIM(c.is_active)=''Y''
AND      TRIM(c.cdc_flag)<>''D''
AND      TRIM(c.clastatus) in (''A'', ''D'')
AND      c.benefitclaind = 1
AND      TRIM(j.IS_ACTIVE) = ''Y''
AND      TRIM(j.CDC_FLAG) <> ''D''

qualify row_number() over (partition by UNIQ_CLM_AUD_NBR,CLM_AUD_LN_NBR
                           order by UNIQ_CLM_AUD_NBR asc , CLM_AUD_LN_NBR ASC, b.BTCH_LOAD_DT DESC,
						    n.CLAIMLINEADJUDICATIONID DESC, i.CHANGEDATETIME DESC, h.UPDATEVERSION DESC,h.CREATEDATETIME DESC) = 1 ;

CREATE OR REPLACE TEMPORARY TABLE TEMP_SRC_QUERY2 AS
SELECT  S.UNIQ_CLM_AUD_NBR,
         S.CLM_AUD_LN_NBR,
		 S.CRR_CLM_SRVC_SYS_ID,
		--S.CLAIMLINEADJUDICATIONID, 
       -- S.CHANGEDATETIME, 
       -- S.UPDATEVERSION,
       -- S.CREATEDATETIME,
         trim(coalesce(l1.MH_SA_CD, '''')) as MH_SA_CD_1,
        trim(coalesce(l2.MH_SA_CD, '''')) as MH_SA_CD_2,
        trim(coalesce(l3.MH_SA_CD, '''')) as MH_SA_CD_3,
       trim(coalesce(l4.MH_SA_CD, '''')) as MH_SA_CD_4,
         
         S.memberid||''-''||o.memgroupid||''-''||o.subscriberid as CLM_MBR_ALT_ID,
        l.ADJUDICATIONDATE as CLM_SRVC_ADJD_DT,
        --S.DERIV_OBH_FINC_LIAB,

      (CASE WHEN S.claimid is not null then coalesce(p1.PROVIDERID,''0'')||''-''||coalesce(p1.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(S.PROVIDERORGID,''0'')||''-''||coalesce(q1.PROVIDERORGLOCATIONID,''0'') else (CASE WHEN S.claimid_G is not null then coalesce(p2.PROVIDERID,''0'')||''-''||coalesce(p2.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(S.PROVIDERORGID,''0'')||''-''||coalesce(q2.PROVIDERORGLOCATIONID,''0'') else ''0-0-''||coalesce(S.PROVIDERORGID,''0'')||''-0'' end) end) as DERIV_SRC_CRR_PROV_ID,
      (CASE WHEN S.claimid is not null then coalesce(p1.PROVIDERID,''0'') else (CASE WHEN S.claimid_G is not null then coalesce(p2.PROVIDERID,''0'') else ''0'' end) end) as PROVIDERID,
         prov_adr.PROV_ADR_CITY_NM,
         prov_adr.PROV_ADR_ST_CD,
         SUBSTR(prov_adr.PROV_ADR_ZIP_CD,1,5) AS PROV_ADR_ZIP_CD,
         prov.PROV_NPI_ID,
         prov.PROV_TAX_ID,
         prov.PROV_CHK_NM,
        (CASE WHEN l.CLAIMEVENTTYPE = ''3'' THEN l.INTERNALADJUSTREASONCODE WHEN S.SVCLINESTATUS_D = ''X'' THEN S.USERCLAIMCANCELCODE_D WHEN S.SVCLINESTATUS_F = ''X'' THEN S.USERCLAIMCANCELCODE_F ELSE '''' END) AS PYMT_EOB_RSN_CD,
        (CASE WHEN l.CLAIMEVENTTYPE = ''3'' THEN IARC.IARCCODEDISPLAY WHEN S.SVCLINESTATUS_D = ''X'' THEN UCCC.DESCRIPTION WHEN S.SVCLINESTATUS_F = ''X'' THEN UCCC1.DESCRIPTION ELSE '''' END) AS PYMT_EOB_RSN_CD_DESC,
CASE WHEN CLBP.PROVIDERPARSTATUSTYPE = ''1'' THEN ''I'' WHEN CLBP.PROVIDERPARSTATUSTYPE = ''2'' THEN ''O'' ELSE '''' END PROVIDERPARSTATUSTYPE,	

CASE WHEN CLBC.BENEFITTIERPARSTATUSTYPE = ''1'' THEN ''NT'' WHEN CLBC.BENEFITTIERPARSTATUSTYPE = ''2'' THEN ''NN'' ELSE '''' END BENEFITTIERPARSTATUSTYPE,

PRV_BIL_NPI.NATLPROVIDERID as CLM_SRC_PROV_CLM_BIL_NPI_ID,
PRV_REF_NPI.NATLPROVIDERID as CLM_SRC_PROV_CLM_REF_NPI_ID,
PRV_REND_NPI.NATLPROVIDERID as CLM_SRC_PROV_CLM_RNDR_NPI_ID,
trim(lx.SERVICE_ID)+trim(lx.AUTH_NBR) as CLM_AUTH_NBR
FROM   TEMP_SRC_QUERY1 S
         left outer join COMPACT.CRR_CLAIMDIAGNOSIS k ON trim(S.claimid) = trim(k.claimid) and (k.is_active=''Y'') and (k.CDC_FLAG <> ''D'' )
		 --left outer join COMPACT.CRR_CLAIMDIAGNOSIS k0 ON trim(c.claimid) = trim(k0.claimid) and (k0.is_active=''Y'')
			 
        left outer join COMPACT.TB_DIAG l1 on replace(coalesce(k.DIAGNOSISCODE, ''''), ''.'','''') = l1.diag_cd and k.DIAGSEQNUM=1 and S.dateofservicestart between l1.DIAG_EFF_DT and l1.DIAG_EXPR_DT and k.is_active=''Y'' 
       left outer join COMPACT.TB_DIAG l2 on replace(coalesce(k.DIAGNOSISCODE, ''''), ''.'','''') = l2.diag_cd and k.DIAGSEQNUM=2 and S.dateofservicestart between l2.DIAG_EFF_DT and l2.DIAG_EXPR_DT and k.is_active=''Y'' 
        left outer join COMPACT.TB_DIAG l3 on replace(coalesce(k.DIAGNOSISCODE, ''''), ''.'','''') = l3.diag_cd and k.DIAGSEQNUM=3 and S.dateofservicestart between l3.DIAG_EFF_DT and l3.DIAG_EXPR_DT and k.is_active=''Y'' 
       left outer join COMPACT.TB_DIAG l4 on replace(coalesce(k.DIAGNOSISCODE, ''''), ''.'','''') = l4.diag_cd and k.DIAGSEQNUM=4 and S.dateofservicestart between l4.DIAG_EFF_DT and l4.DIAG_EXPR_DT and k.is_active=''Y'' 
         left outer join COMPACT.CRR_CLAIMEVENT l on S.claimEventID = l.claimEventID and l.is_active=''Y'' AND l.CDC_FLAG <> ''D'' 
         --left outer join COMPACT.CRR_CLABENEFITALLOWED n on S.claimLineAdjudicationID = n.claimLineAdjudicationID and n.is_active=''Y'' AND n.CDC_FLAG <> ''D'' 
         left outer join COMPACT.CRR_CLAIMSUBSCRIBER o on trim(S.claimid_Z) = trim(o.claimid) and S.claimsubscriberid = o.claimsubscriberid and o.is_active = ''Y'' and o.CDC_FLAG <> ''D'' 
         left outer join COMPACT.CRR_CLAIMPROVIDER p1 on trim(S.claimid_Z) = trim(p1.claimid) and S.renderingclaimproviderid = p1.claimproviderid and p1.is_active=''Y'' and p1.CDC_FLAG <> ''D'' 
         left outer join COMPACT.CRR_CLAIMPROVIDERADDRESS q1 on trim(S.claimid_Z) = trim(q1.claimid) and S.renderingclaimprovideraddressid = q1.claimprovideraddressid and q1.is_active=''Y'' and q1.CDC_FLAG <> ''D'' 
         left outer join COMPACT.CRR_CLAIMPROVIDER p2 on trim(S.claimid_Z) = trim(p2.claimid) and S.renderingclaimproviderid_G = p2.claimproviderid and p2.is_active=''Y'' and p2.CDC_FLAG <> ''D'' 
         left outer join COMPACT.CRR_CLAIMPROVIDERADDRESS q2 on trim(S.claimid_Z) = trim(q2.claimid) and S.renderingclaimprovideraddressid_G = q2.claimprovideraddressid and q2.is_active=''Y'' and q2.CDC_FLAG <> ''D'' 
         LEFT OUTER JOIN COMPACT.crr_clabenefitprovider CLBP on S.claimlineadjudicationid = CLBP.claimlineadjudicationid AND CLBP.is_active = ''Y'' and CLBP.cdc_flag <>''D''

         LEFT OUTER JOIN  COMPACT.crr_clabenefitcode CLBC on S.claimlineadjudicationid = CLBC.claimlineadjudicationid AND CLBC.is_active = ''Y'' and CLBC.cdc_flag <>''D''
         LEFT OUTER JOIN COMPACT.CRR_USERCLAIMCANCELCODE UCCC ON S.USERCLAIMCANCELCODE_G = UCCC.USERCLAIMCANCELCODE AND UCCC.IS_ACTIVE = ''Y'' AND UCCC.CDC_FLAG <> ''D'' 
         LEFT OUTER JOIN COMPACT.CRR_USERCLAIMCANCELCODE UCCC1 ON S.USERCLAIMCANCELCODE_F = UCCC1.USERCLAIMCANCELCODE AND UCCC1.IS_ACTIVE = ''Y'' AND UCCC1.CDC_FLAG <> ''D'' 
         LEFT OUTER JOIN COMPACT.CRR_INTERNALADJUSTREASONCODE IARC ON l.INTERNALADJUSTREASONCODE = IARC.INTERNALADJUSTREASONCODE AND IARC.IS_ACTIVE = ''Y'' AND IARC.CDC_FLAG <> ''D'' 
         left outer join COMPACT.TB_PRV_PROVIDER_ADR prov_adr on trim(prov_adr.src_prov_adr_id)=trim(substr(DERIV_SRC_CRR_PROV_ID,position(''-'',DERIV_SRC_CRR_PROV_ID,-1)+1,length(nvl(DERIV_SRC_CRR_PROV_ID,'''')))) and prov_adr.REC_INPUT_SRC_CD=''CRR'' 
         left outer join COMPACT.TB_PRV_PROVIDER prov on trim(prov.src_prov_id)=trim(DERIV_SRC_CRR_PROV_ID) and prov.REC_INPUT_SRC_CD=''CRR'' 
         Left outer join COMPACT.TB_CLM_CRR_CLM_SRVC p on TRIM(p.CLM_AUD_NBR) = TRIM(S.CLAIMID) and TRIM(p.CLM_AUD_LN_NBR) = RIGHT(''000''||TRIM(S.CLAIMLINENUM),3)

--CRR NPI

LEFT OUTER JOIN COMPACT.CRR_CLAIMPROVIDER PRV_BIL_NPI ON TRIM(S.claimid_Z) = TRIM(PRV_BIL_NPI.claimid) 
 AND TRIM(PRV_BIL_NPI.ENTITYIDCODE) = ''85'' 
AND TRIM(PRV_BIL_NPI.is_active)=''Y'' and TRIM(PRV_BIL_NPI.CDC_FLAG) <> ''D''
LEFT OUTER JOIN COMPACT.CRR_CLAIMPROVIDER PRV_REND_NPI ON TRIM(S.claimid_Z) = TRIM(PRV_REND_NPI.claimid) 
 AND TRIM(PRV_REND_NPI.ENTITYIDCODE) = ''82'' 
AND TRIM(PRV_REND_NPI.is_active)=''Y'' and TRIM(PRV_REND_NPI.CDC_FLAG) <> ''D''
LEFT OUTER JOIN COMPACT.CRR_CLAIMPROVIDER PRV_REF_NPI ON TRIM(S.claimid_Z) = TRIM(PRV_REF_NPI.claimid) 
          AND TRIM(PRV_REF_NPI.ENTITYIDCODE) = ''DN'' AND TRIM(PRV_REF_NPI.is_active)=''Y'' and TRIM(PRV_REF_NPI.CDC_FLAG) <> ''D''
--AUTH_NBR
--USP
Left outer join  COMPACT.T_AUTH_CLAIM_INFO lx on 
 TRIM(lx.xref_id) = TRIM(S.CLM_SRVC_AUTH_NBR) and TRIM(lx.xref_type_code) = ''SRN'' --join23
 qualify row_number() over (partition by S.UNIQ_CLM_AUD_NBR,S.CLM_AUD_LN_NBR
                           order by S.UNIQ_CLM_AUD_NBR asc , S.CLM_AUD_LN_NBR ASC,
						    S.CLAIMLINEADJUDICATIONID DESC, S.CHANGEDATETIME DESC, S.UPDATEVERSION DESC,S.CREATEDATETIME DESC) = 1;


					
					
CREATE or replace TEMPORARY TABLE TEMP_extSTG_CRR_CLAIMHEADER AS		
SELECT 
 C.BTCH_LOAD_DT,
C.UNIQ_CLM_AUD_NBR,
C.SEQ_NO,
(case when C.claimtype = 2 then f.DATEOFSERVICEFROM
      when C.claimtype = 3 then g.DATEOFSERVICEFROM
      else '''' end) AS SRVC_STRT_DT,
(case when C.claimtype = 2 then f.DATEOFSERVICETHRU
      when C.claimtype = 3 then g.DATEOFSERVICETHRU 
      else '''' end) AS SRVC_END_DT,
C.CLM_FM_TYP_CD,
(Case When C.CLM_FM_TYP_CD =''1'' and f.REVENUECODE is not null Then (Case When left(trim(f.REVENUECODE),1) =''0'' Then right(trim(f.REVENUECODE),3) Else trim(f.REVENUECODE) End) 
                      Else (case when C.claimtype=''2'' then Left(trim(f.PROCEDURECODE),5)
      when C.claimtype=''3'' then Left(trim(g.PROCEDURECODE),5)
      else '''' end) End) as DRV_PROC_CD,
           Trim(Coalesce(TB_PROC.PROC_TYP_CD,'''')) as DRV_PROC_TYP_CD,
           Coalesce(TB_PROC.PROC_CPT_TYP_CD, ''NA'') as DRV_PROC_CPT_TYP_CD, 

 coalesce(PRV_PROVIDER_ALT.PROV_SYS_ID,-1) as PROV_SYS_ID,    
           Coalesce(trim(prov.PROV_REIM_CATGY_NM),'''') AS PROV_REIM_CATGY_NM,
           Coalesce(POS.POS_DERIV_CATGY, ''NA'') as POS_DERIV_CATGY,
(CASE WHEN e1.claimid is not null then coalesce(p1.PROVIDERID,''0'')||''-''||coalesce(p1.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(C.PROVIDERORGID,''0'')||''-''||coalesce(q1.PROVIDERORGLOCATIONID,''0'') else
(CASE WHEN f1.claimid is not null then coalesce(p2.PROVIDERID,''0'')||''-''||coalesce(p2.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(C.PROVIDERORGID,''0'')||''-''||coalesce(q2.PROVIDERORGLOCATIONID,''0'') else ''0-0-''||coalesce(C.PROVIDERORGID,''0'')||''-0''
end) end) as DERIV_SRC_CRR_PROV_ID
--CASE WHEN C.SEQ_NO = e1.claimLineNum and TRIM(C.claimtype)=''2'' and e1.is_active=''Y'' THEN e1.DATEOFSERVICEFROM ELSE NULL END AS DATEOFSERVICEFROM_F
FROM(
select DISTINCT
 c.BTCH_LOAD_DT as BTCH_LOAD_DT,
TRIM(c.CLAIMID) AS UNIQ_CLM_AUD_NBR,
c.CLAIMLINENUM AS SEQ_NO,
z.claimtype,
b.CLM_FM_TYP_CD,
z.PROVIDERORGID,
c.claimeventid
           
--(CASE WHEN e1.claimid is not null then coalesce(p1.PROVIDERID,''0'')||''-''||coalesce(p1.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(z.PROVIDERORGID,''0'')||''-''||coalesce(q1.PROVIDERORGLOCATIONID,''0'') else
--(CASE WHEN f1.claimid is not null then coalesce(p2.PROVIDERID,''0'')||''-''||coalesce(p2.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(z.PROVIDERORGID,''0'')||''-''||coalesce(q2.PROVIDERORGLOCATIONID,''0'') else ''0-0-''||coalesce(z.PROVIDERORGID,''0'')||''-0''
--end) end) as DERIV_SRC_CRR_PROV_ID
from
-- COMPACT.TB_CLM_CRR_BH_CLM_WRK a 
-- inner join 
COMPACT.TB_CLM_CRR_CLM_HEAD b
-- on a.CLM_AUD_NBR=b.CLM_AUD_NBR 
inner join COMPACT.CRR_CLAIMHEADER z on b.CLM_AUD_NBR = TRIM(z.claimid)
inner join COMPACT.CRR_CLAIMLINEADJUDICATION c on TRIM(z.claimid) = TRIM(c.claimid)
inner join COMPACT.CRR_CLABENEFIT j on c.claimlineadjudicationid = j.claimlineadjudicationid
where
c.is_active=''Y''   
and c.cdc_flag<>''D''
and c.clastatus in (''A'', ''D'')
and c.benefitclaind = 1 
and z.is_active=''Y''
and j.is_active=''Y'') AS C
left outer join COMPACT.CRR_CLAIMLINEINST f on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(f.claimid) and C.SEQ_NO = f.claimLineNum 
 and TRIM(C.claimtype)=''2'' and f.is_active=''Y''
left outer join COMPACT.CRR_CLAIMLINEPROF g on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(g.claimid) and C.SEQ_NO = g.claimLineNum
 and  TRIM(C.claimtype)=''3'' and g.is_active=''Y''
left outer join COMPACT.CRR_CLAIMLINEINST e1 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(e1.claimid) and e1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDER p1 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(p1.claimid) and 
TRIM(e1.renderingclaimproviderid) = TRIM(p1.claimproviderid) and p1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDERADDRESS q1 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(q1.claimid) and e1.renderingclaimprovideraddressid = q1.claimprovideraddressid 
and q1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMLINEPROF f1 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(f1.claimid) and f1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDER p2 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(p2.claimid) and f1.renderingclaimproviderid = p2.claimproviderid and p2.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDERADDRESS q2 on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(q2.claimid) 
and TRIM(f1.renderingclaimprovideraddressid) = TRIM(q2.claimprovideraddressid) 
and q2.is_active=''Y''
LEFT OUTER JOIN COMPACT.TB_AMA_POS POS ON POS.POS_ID = trim(coalesce(g.POSCODE,''''))
LEFT OUTER JOIN COMPACT.TB_PROC TB_PROC ON DRV_PROC_CD = TB_PROC.PROC_CD AND C.CLM_FM_TYP_CD = TB_PROC.CLM_FM_TYP_CD
left outer join COMPACT.TB_PRV_PROVIDER_ALT PRV_PROVIDER_ALT on PRV_PROVIDER_ALT.prov_alt_id = DERIV_SRC_CRR_PROV_ID and TRIM(PRV_PROVIDER_ALT.REC_INPUT_SRC_CD)=''CRR''
left outer join COMPACT.TB_PRV_PROVIDER prov on trim(prov.src_prov_id)=trim(DERIV_SRC_CRR_PROV_ID) and prov.REC_INPUT_SRC_CD=''CRR''
left outer join COMPACT.CRR_APTRANSACTION h on TRIM(C.UNIQ_CLM_AUD_NBR) = TRIM(h.claimid) and C.claimeventid = h.claimeventid and h.is_active=''Y'' and h.CDC_FLAG <> ''D'' 
left outer join COMPACT.CRR_APGENERATIONRESPONSE i on h.apTransactionID = i.apTransactionID 
                                                    and i.is_active=''Y'' and i.CDC_FLAG <> ''D''  AND i.OFFSETIND <> 1
                                                    
                                                    
qualify row_number() over (partition by C.UNIQ_CLM_AUD_NBR,C.SEQ_NO
                           order by C.UNIQ_CLM_AUD_NBR asc , C.SEQ_NO ASC, C.BTCH_LOAD_DT DESC) = 1  ;
						   
						   

CREATE or replace TEMPORARY TABLE TEMP_extSTG_CRR_CLAIMHEADER_12 AS	
(SELECT 
 BTCH_LOAD_DT,
UNIQ_CLM_AUD_NBR,
 SEQ_NO,
 SRVC_STRT_DT,
SRVC_END_DT,
CLM_FM_TYP_CD,
DRV_PROC_CD AS PROC_CD,
  DRV_PROC_TYP_CD AS PROC_TYP_CD,
 DRV_PROC_CPT_TYP_CD AS PROC_CPT_TYP_CD, 
 PROV_SYS_ID,    
 PROV_REIM_CATGY_NM,
 POS_DERIV_CATGY,
 DERIV_SRC_CRR_PROV_ID AS SRC_CRR_PROV_ID
 FROM TEMP_extSTG_CRR_CLAIMHEADER) ; 


CREATE or replace TEMPORARY TABLE TEMP_extSTG_CRR_CLAIMHEADER_PRMY AS
SELECT
C.BTCH_LOAD_DT,
C.CLM_SRVC_UNIQ_ID,
C.UNIQ_CLM_AUD_NBR,
C.CLM_AUD_LN_NBR,
(case when C.claimtype = 2 then f.DATEOFSERVICEFROM
      when C.claimtype = 3 then g.DATEOFSERVICEFROM 
      else '''' end) AS SRVC_STRT_DT,
(case when C.claimtype = 2 then f.DATEOFSERVICETHRU
      when C.claimtype = 3 then g.DATEOFSERVICETHRU 
      else '''' end) AS SRVC_END_DT,
C.CLM_FM_TYP_CD,
(Case When C.CLM_FM_TYP_CD =1 and f.REVENUECODE is not null Then (Case When left(trim(f.REVENUECODE),1) =0 Then right(trim(f.REVENUECODE),3) Else trim(f.REVENUECODE) End) 
                      Else (case when C.claimtype=''2'' then Left(trim(f.PROCEDURECODE),5)
      when C.claimtype=3 then Left(trim(g.PROCEDURECODE),5)
      else '''' end) End) as DRV_PROC_CD,
           Trim(IFNULL(TB_PROC.PROC_TYP_CD,'''')) as DRV_PROC_TYP_CD,
           IFNULL(TB_PROC.PROC_CPT_TYP_CD, ''NA'') as DRV_PROC_CPT_TYP_CD, 
          IFNULL(PRV_PROVIDER_ALT.PROV_SYS_ID,-1) as PROV_SYS_ID,    
           IFNULL(trim(prov.PROV_REIM_CATGY_NM),'''') AS PROV_REIM_CATGY_NM,
           IFNULL(POS.POS_DERIV_CATGY, ''NA'') as DRV_POS_DERIV_CATGY,
          1 as Primary_CPT,
           (Case When p.LVL_OF_CARE_DERIV_TXT = ''EAP'' Then 1 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Acute Inpatient'' Then 2 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Residential'' Then 3 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Day Treatment'' Then 4 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Structured Outpatient'' Then 5 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Recovery Home'' Then 6 
                       When p.LVL_OF_CARE_DERIV_TXT = ''ECT'' Then 7 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Outpatient'' Then 8 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Medication Services'' Then 9 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Professional Services'' Then 10 
                       When p.LVL_OF_CARE_DERIV_TXT = ''Ancillary'' Then 11
                       Else 12  End) as DLOC_TXT_Prec,
(CASE WHEN e1.claimid is not null then coalesce(p1.PROVIDERID,''0'')||''-''||coalesce(p1.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(C.PROVIDERORGID,''0'')||''-''||coalesce(q1.PROVIDERORGLOCATIONID,''0'') else
(CASE WHEN f1.claimid is not null then coalesce(p2.PROVIDERID,''0'')||''-''||coalesce(p2.PROVIDERORGAFFILIATIONID,''0'')||''-''||coalesce(C.PROVIDERORGID,''0'')||''-''||coalesce(q2.PROVIDERORGLOCATIONID,''0'') else ''0-0-''||coalesce(C.PROVIDERORGID,''0'')||''-0''
end) end) as DERIV_SRC_CRR_PROV_ID
FROM(
select DISTINCT
 c.BTCH_LOAD_DT as BTCH_LOAD_DT,
  '''' as CLM_SRVC_UNIQ_ID,
c.CLAIMID AS UNIQ_CLM_AUD_NBR,
c.CLAIMLINENUM AS CLM_AUD_LN_NBR,
z.claimtype,
b.CLM_FM_TYP_CD,
b.BTCH_LOAD_DT AS BTCHLOADDT,
z.PROVIDERORGID,
c.claimeventid

from
-- COMPACT.TB_CLM_CRR_BH_CLM_WRK a 
-- inner join 
COMPACT.TB_CLM_CRR_CLM_HEAD b
-- on a.CLM_AUD_NBR=b.CLM_AUD_NBR 
inner join COMPACT.CRR_CLAIMHEADER z on b.CLM_AUD_NBR = z.CLAIMID
inner join COMPACT.CRR_CLAIMLINEADJUDICATION c on TRIM(z.claimid) = TRIM(c.claimid) 
inner join COMPACT.CRR_CLABENEFIT j on c.claimlineadjudicationid = j.claimlineadjudicationid

where
c.is_active=''Y''   
and c.cdc_flag<>''D''
and c.clastatus in (''A'', ''D'')
and c.benefitclaind = 1 
and z.is_active=''Y''
and j.is_active=''Y''
) AS C
left outer join COMPACT.CRR_CLAIMLINEINST f on C.UNIQ_CLM_AUD_NBR = TRIM(f.claimid) and c.CLM_AUD_LN_NBR = f.claimLineNum and C.claimtype=2 and f.is_active=''Y''
left outer join COMPACT.CRR_CLAIMLINEPROF g on C.UNIQ_CLM_AUD_NBR = TRIM(g.claimid) and c.CLM_AUD_LN_NBR = g.claimLineNum and  C.claimtype=3 and g.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMLINEINST e1 on C.UNIQ_CLM_AUD_NBR = TRIM(e1.claimid) and e1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDER p1 on C.UNIQ_CLM_AUD_NBR = TRIM(p1.claimid) and 
TRIM(e1.renderingclaimproviderid) = TRIM(p1.claimproviderid) and p1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDERADDRESS q1 on C.UNIQ_CLM_AUD_NBR = TRIM(q1.claimid) and 
TRIM(e1.renderingclaimprovideraddressid) = TRIM(q1.claimprovideraddressid) 
and q1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMLINEPROF f1 on C.UNIQ_CLM_AUD_NBR = TRIM(f1.claimid) and f1.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDER p2 on C.UNIQ_CLM_AUD_NBR = TRIM(p2.claimid)and 
TRIM(f1.renderingclaimproviderid) = TRIM(p2.claimproviderid) and p2.is_active=''Y''
left outer join  COMPACT.CRR_CLAIMPROVIDERADDRESS q2 on C.UNIQ_CLM_AUD_NBR = TRIM(q2.claimid) and 
TRIM(f1.renderingclaimprovideraddressid) = TRIM(q2.claimprovideraddressid) 
and q2.is_active=''Y''
LEFT OUTER JOIN COMPACT.TB_AMA_POS POS ON POS.POS_ID = trim(IFNULL(g.POSCODE,''''))
LEFT OUTER JOIN COMPACT.TB_PROC TB_PROC ON TRIM(DRV_PROC_CD) = TRIM(TB_PROC.PROC_CD) AND TRIM(C.CLM_FM_TYP_CD) = TRIM(TB_PROC.CLM_FM_TYP_CD) 
left outer join COMPACT.TB_PRV_PROVIDER_ALT PRV_PROVIDER_ALT on TRIM(PRV_PROVIDER_ALT.prov_alt_id) = TRIM(DERIV_SRC_CRR_PROV_ID) and TRIM(PRV_PROVIDER_ALT.REC_INPUT_SRC_CD)=''CRR''
left outer join COMPACT.TB_PRV_PROVIDER prov on trim(prov.src_prov_id)=trim(DERIV_SRC_CRR_PROV_ID) and prov.REC_INPUT_SRC_CD=''CRR''
Left outer join COMPACT.TB_CLM_CRR_CLM_SRVC p
on  TRIM(p.CLM_AUD_NBR) = C.UNIQ_CLM_AUD_NBR and TRIM(p.CLM_AUD_LN_NBR) = RIGHT(''000''||TRIM(c.CLM_AUD_LN_NBR),3)
WHERE DRV_PROC_CPT_TYP_CD IN (''CPT2013'', ''EM'', ''EM/MD'')
qualify row_number() over (partition by C.UNIQ_CLM_AUD_NBR,SRVC_STRT_DT,SRVC_END_DT
order by C.UNIQ_CLM_AUD_NBR asc , SRVC_STRT_DT ASC, SRVC_END_DT ASC ,DLOC_TXT_PREC ASC, C.BTCHLOADDT DESC) = 1 ;

                                                   
						   
					   

CREATE or replace TEMPORARY TABLE TEMP_extSTG_CRR_CLAIMHEADER_PRMY_12 AS
(select BTCH_LOAD_DT,
 CLM_SRVC_UNIQ_ID,
UNIQ_CLM_AUD_NBR,
CLM_AUD_LN_NBR,
SRVC_STRT_DT,
SRVC_END_DT,
CLM_FM_TYP_CD,
 DRV_PROC_CD AS PROC_CD,
 DRV_PROC_TYP_CD AS PROC_TYP_CD,
DRV_PROC_CPT_TYP_CD AS PROC_CPT_TYP_CD, 
 PROV_SYS_ID,    
 PROV_REIM_CATGY_NM,
DRV_POS_DERIV_CATGY AS POS_DERIV_CATGY,
Primary_CPT,
 DLOC_TXT_Prec,
 DERIV_SRC_CRR_PROV_ID AS SRC_CRR_PROV_ID
FROM TEMP_extSTG_CRR_CLAIMHEADER_PRMY ) ; 						   




CREATE OR REPLACE TEMPORARY TABLE TEMP_CrrfstClmProcCd AS
( SELECT  UNIQ_CLM_AUD_NBR, SRVC_STRT_DT, SRVC_END_DT,
  CASE WHEN (((( PROC_CPT_TYP_CD = ''CPT2012'' AND (PROC_CD = ''90801'' OR PROC_CD =''90806'')))  OR PROC_CPT_TYP_CD = ''EM/MD'')
                      AND (PROV_REIM_CATGY_NM = ''Medical MD'' OR PROV_REIM_CATGY_NM = ''MD'')) THEN PROC_CD||''MD''
                WHEN ((PROC_CPT_TYP_CD = ''AO'' OR PROC_CPT_TYP_CD = ''AO/MD'') AND PRIMARY_CPT = 1) THEN PROC_CD
                WHEN (PROC_CPT_TYP_CD = ''CPT2013'' OR (PROC_CPT_TYP_CD = ''AO'' AND PRIMARY_CPT != 1 ) OR
                                                (PROC_CPT_TYP_CD = ''AO/MD'' AND PRIMARY_CPT != 1 AND PROV_REIM_CATGY_NM != ''MD'')) THEN
                                                CASE WHEN POS_DERIV_CATGY  = ''Ancillary'' THEN
 (CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''ANC''
WHEN POS_DERIV_CATGY  = ''Professional Services'' THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''PS''
WHEN POS_DERIV_CATGY  = ''Outpatient or Medication Services'' THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''OMS''
WHEN (POS_DERIV_CATGY  = ''NA'' || POS_DERIV_CATGY  = '''') THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''NA''
END
                WHEN (PROC_CPT_TYP_CD = ''AO/MD'' AND PRIMARY_CPT != 1 AND PROV_REIM_CATGY_NM = ''MD'' ) THEN PROC_CD||''MDNANA''
                ELSE PROC_CD
                END AS PROC_CD_CPT
                FROM TEMP_extSTG_CRR_CLAIMHEADER_PRMY_12 );				   

		
		   
CREATE OR REPLACE TEMPORARY TABLE TEMP_jn_CPT_t1xfrm_MAP_1 AS
(SELECT '''' AS CLM_SRVC_UNIQ_ID, A.UNIQ_CLM_AUD_NBR, 
A.SEQ_NO AS CLM_AUD_LN_NBR, A.SRVC_STRT_DT AS FROM_DT, A.SRVC_END_DT AS TO_DT, A.CLM_FM_TYP_CD, 
CASE WHEN (A.PROC_CPT_TYP_CD = ''AO'' Or A.PROC_CPT_TYP_CD = ''AO/MD'' and (Length(B.PROC_CD_CPT) > 0)) Then B.PROC_CD_CPT  Else A.PROC_CD END AS PROC_CD ,
A.PROC_TYP_CD, A.PROC_CPT_TYP_CD, A.PROV_SYS_ID, A.PROV_REIM_CATGY_NM, A.POS_DERIV_CATGY, 
CASE WHEN Len(B.PROC_CD_CPT) > 0 Then 1 Else 0 END AS PRIMARY_CPT
FROM 
TEMP_extSTG_CRR_CLAIMHEADER_12 A LEFT OUTER JOIN TEMP_CrrfstClmProcCd B ON 
TRIM(A.UNIQ_CLM_AUD_NBR) = TRIM(B.UNIQ_CLM_AUD_NBR) AND 
TRIM(A.SRVC_STRT_DT) = TRIM(B.SRVC_STRT_DT) AND 
TRIM(A.SRVC_END_DT) = TRIM(B.SRVC_END_DT) ) ;     



CREATE OR REPLACE TEMPORARY TABLE TEMP_CrrfstClmProcCd_1 AS
( SELECT UNIQ_CLM_AUD_NBR,CLM_AUD_LN_NBR,
  PROC_TYP_CD, PROV_SYS_ID, PROC_CPT_TYP_CD,
  CASE WHEN (((( PROC_CPT_TYP_CD = ''CPT2012'' AND (PROC_CD = ''90801'' OR PROC_CD =''90806'')))  OR PROC_CPT_TYP_CD = ''EM/MD'')
                      AND (PROV_REIM_CATGY_NM = ''Medical MD'' OR PROV_REIM_CATGY_NM = ''MD'')) THEN PROC_CD||''MD''
                WHEN ((PROC_CPT_TYP_CD = ''AO'' OR PROC_CPT_TYP_CD = ''AO/MD'') AND PRIMARY_CPT = 1) THEN PROC_CD
                WHEN (PROC_CPT_TYP_CD = ''CPT2013'' OR (PROC_CPT_TYP_CD = ''AO'' AND PRIMARY_CPT != 1 ) OR
                                                (PROC_CPT_TYP_CD = ''AO/MD'' AND PRIMARY_CPT != 1 AND PROV_REIM_CATGY_NM != ''MD'')) THEN
                                                CASE WHEN POS_DERIV_CATGY  = ''Ancillary'' THEN
 (CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''ANC''
WHEN POS_DERIV_CATGY  = ''Professional Services'' THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''PS''
WHEN POS_DERIV_CATGY  = ''Outpatient or Medication Services'' THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''OMS''
WHEN (POS_DERIV_CATGY  = ''NA'' || POS_DERIV_CATGY  = '''') THEN
(CASE WHEN CLM_FM_TYP_CD = ''1'' THEN
(case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''FA''
ELSE (case when (PROC_CD = ''90792'' and PROV_REIM_CATGY_NM = ''MD'') then PROC_CD||''MD'' ELSE PROC_CD||''NA'' END)||''PR''
END )||''NA''
END
                WHEN (PROC_CPT_TYP_CD = ''AO/MD'' AND PRIMARY_CPT != 1 AND PROV_REIM_CATGY_NM = ''MD'' ) THEN PROC_CD||''MDNANA''
                ELSE PROC_CD
                END AS PROC_CD_CPT
                FROM TEMP_jn_CPT_t1xfrm_MAP_1 );                    



CREATE OR REPLACE TEMPORARY TABLE TEMP_jn_stg AS
(SELECT A.UNIQ_CLM_AUD_NBR, A.CLM_AUD_LN_NBR, A.CRR_CLM_SRVC_SYS_ID, A.CLAIMID,
A.BTCH_LOAD_DT, A.CLM_SRVC_ADJ_CD, A.CLM_SRVC_CLM_STS_CD, A.SRVC_STRT_DT, A.SRVC_END_DT,
A.CLM_SRVC_PD_DT, A.CLM_SRVC_PST_DT, C.CLM_SRVC_ADJD_DT, A.ALLOWEDAMT, A.COBADJUSTMENTAMT,
A.MEMCOINSURANCEREDUCTAMT, A.COPAYREDUCTAMT, A.DEDUCTIBLEREDUCTAMT, 
A.PROMPTPAYDISCOUNTREDUCTAMT, A.BILLEDANDTAXAMT, A.PRICEDAMT, A.NONCOVEREDAMT,
A.FINALMEMRESPAMT, A.FINALNETPAYAMT, A.CLM_SRVC_UNIT_CNT, A.CLM_SRVC_UNIT_DERIV_CNT,
A.CLM_SRVC_DIAG_ADMIT_CD, A.CLM_SRVC_DIAG_1_CD, A.CLM_SRVC_DIAG_2_CD, A.CLM_SRVC_DIAG_3_CD,
A.CLM_SRVC_DIAG_4_CD, A.CLM_SRVC_DIAG_5_CD, A.CLM_SRVC_DIAG_6_CD, A.CLM_SRVC_DIAG_7_CD,
A.CLM_SRVC_DIAG_8_CD, A.CLM_SRVC_DIAG_9_CD, A.CLM_SRVC_DIAG_10_CD, A.CLM_SRVC_PROC_MOD_CD,
A.CLM_SRVC_PROC_MOD_2_CD, A.CLM_SRVC_PROC_MOD_3_CD, A.CLM_SRVC_PROC_MOD_4_CD, C.MH_SA_CD_1,
C.MH_SA_CD_2,C.MH_SA_CD_3, C.MH_SA_CD_4,A.LINEOFBUSINESSID, A.CLM_MBR_SRC_MBR_SYS_ID,
A.CHECKNUM, A.PAYEEENTITYTYPE,
 A.CLM_SRVC_PROC_CD, A.ICD_DIAG_TYP_CD, C.CLM_MBR_ALT_ID, A.SRC_REC_CRT_DTTM,A.SRC_REC_UPDT_DTTM,
 A.CRR_CLM_HEAD_SYS_ID,A.CLM_TYP_CD,A.CLM_FM_TYP_CD,B.PROV_SYS_ID, A.POSCODE,
 A.REVENUECODE, B.PROC_TYP_CD, B.PROC_CPT_TYP_CD, B.PROC_CD_CPT, A.SRC_FINC_LIAB_CD,
 A.DERIV_OBH_FINC_LIAB, C.DERIV_SRC_CRR_PROV_ID AS SRC_CRR_PROV_ID, C.PROVIDERID, C.PROV_ADR_CITY_NM,
 C.PROV_ADR_ST_CD,C.PROV_ADR_ZIP_CD, C.PROV_NPI_ID, C.PROV_TAX_ID, C.PROV_CHK_NM,
 C.PYMT_EOB_RSN_CD, C.PYMT_EOB_RSN_CD_DESC, C.PROVIDERPARSTATUSTYPE, 
 C.BENEFITTIERPARSTATUSTYPE, C.CLM_SRC_PROV_CLM_BIL_NPI_ID, C.CLM_SRC_PROV_CLM_REF_NPI_ID,
C.CLM_SRC_PROV_CLM_RNDR_NPI_ID, C.CLM_AUTH_NBR
FROM TEMP_SRC_QUERY1 A 
LEFT OUTER JOIN  TEMP_SRC_QUERY2 C ON TRIM(A.UNIQ_CLM_AUD_NBR) = TRIM(C.UNIQ_CLM_AUD_NBR) AND TRIM(A.CLM_AUD_LN_NBR) = TRIM(C.CLM_AUD_LN_NBR)
LEFT OUTER JOIN TEMP_CrrfstClmProcCd_1 B ON 
TRIM(A.UNIQ_CLM_AUD_NBR) = TRIM(B.UNIQ_CLM_AUD_NBR) AND TRIM(A.CLM_AUD_LN_NBR) = TRIM(B.CLM_AUD_LN_NBR)) ;



CREATE OR REPLACE TEMPORARY TABLE TEMP_t1xfrm_MAP_2 AS
(SELECT 
UNIQ_CLM_AUD_NBR, CLM_AUD_LN_NBR, CRR_CLM_SRVC_SYS_ID, CLAIMID,
BTCH_LOAD_DT, CLM_SRVC_ADJ_CD, CLM_SRVC_CLM_STS_CD, SRVC_STRT_DT, SRVC_END_DT,
CLM_SRVC_PD_DT, CLM_SRVC_PST_DT, CLM_SRVC_ADJD_DT, ALLOWEDAMT, COBADJUSTMENTAMT,
MEMCOINSURANCEREDUCTAMT, COPAYREDUCTAMT, DEDUCTIBLEREDUCTAMT, 
PROMPTPAYDISCOUNTREDUCTAMT, BILLEDANDTAXAMT, PRICEDAMT, NONCOVEREDAMT,
FINALMEMRESPAMT, FINALNETPAYAMT, CLM_SRVC_UNIT_CNT, CLM_SRVC_UNIT_DERIV_CNT,
CLM_SRVC_DIAG_ADMIT_CD, CLM_SRVC_DIAG_1_CD, CLM_SRVC_DIAG_2_CD, CLM_SRVC_DIAG_3_CD,
CLM_SRVC_DIAG_4_CD, CLM_SRVC_DIAG_5_CD, CLM_SRVC_DIAG_6_CD, CLM_SRVC_DIAG_7_CD,
CLM_SRVC_DIAG_8_CD, CLM_SRVC_DIAG_9_CD, CLM_SRVC_DIAG_10_CD, CLM_SRVC_PROC_MOD_CD,
CLM_SRVC_PROC_MOD_2_CD, CLM_SRVC_PROC_MOD_3_CD, CLM_SRVC_PROC_MOD_4_CD, MH_SA_CD_1,
MH_SA_CD_2,MH_SA_CD_3, MH_SA_CD_4,LINEOFBUSINESSID, CLM_MBR_SRC_MBR_SYS_ID,
CHECKNUM, PAYEEENTITYTYPE,
 CLM_SRVC_PROC_CD, ICD_DIAG_TYP_CD, CLM_MBR_ALT_ID, SRC_REC_CRT_DTTM,SRC_REC_UPDT_DTTM,
 CRR_CLM_HEAD_SYS_ID,CLM_TYP_CD,CLM_FM_TYP_CD,
 NVL(REVENUECODE,'''') AS RCRC_ID,
 NVL(CLM_SRVC_PROC_CD,'''') AS IPCD_ID,
 CASE WHEN PROV_SYS_ID = 0 Then -1 Else PROV_SYS_ID END AS PROV_SYS_ID,
 CASE WHEN trim(CLM_FM_TYP_CD) = ''1'' And Len(case when substr(NVL(REVENUECODE,''''),1,1) = ''0'' Then substr(NVL(REVENUECODE,''''),2,3) Else NVL(REVENUECODE,'''') end) > 0 
 Then (case when substr(NVL(REVENUECODE,''''),1,1) = ''0'' Then substr(NVL(REVENUECODE,''''),2,3) Else NVL(REVENUECODE,'''') end) Else NVL(CLM_SRVC_PROC_CD,'''') end as PROC_CD,
 POSCODE,
 REVENUECODE,  TRIM(PROC_CPT_TYP_CD) PROC_CPT_TYP_CD, trim(PROC_CD_CPT) as DLOC_PROC_CD,
 ''-1'' AS AUTH_SYS_ID,
 ''NA'' AS AUTH_LVL_OF_SRVC_CD,
 ''NA'' AS AUTH_LVL_OF_CARE_TXT,
 ''NA'' AS AUTH_NBR,
CASE WHEN Len(trim(PROC_TYP_CD)) >0 Then trim(PROC_TYP_CD) WHEN Len(trim(PROC_CD_CPT)) = 3 Then ''REV'' WHEN Len(trim(PROC_CD_CPT)) = 4 And SUBSTR(trim(PROC_CD_CPT),1,1) = ''1'' Then ''REV'' WHEN Len(trim(PROC_CD_CPT)) = 4 And SUBSTR(trim(PROC_CD_CPT),1,1) = ''0'' Then ''REV'' WHEN len(trim(PROC_CD_CPT))>0 Then 
 (CASE WHEN IS_CHAR(TO_VARIANT(SUBSTR(trim(PROC_CD_CPT),1,1))) Then ''HCPCS'' Else ''CPT4'' END)
 Else ''CPT4'' END AS PROC_TYP_CD,

 SRC_FINC_LIAB_CD,
 DERIV_OBH_FINC_LIAB, SRC_CRR_PROV_ID, PROVIDERID, PROV_ADR_CITY_NM,
 PROV_ADR_ST_CD,PROV_ADR_ZIP_CD, PROV_NPI_ID, PROV_TAX_ID, PROV_CHK_NM,
 PYMT_EOB_RSN_CD, PYMT_EOB_RSN_CD_DESC, PROVIDERPARSTATUSTYPE, 
 BENEFITTIERPARSTATUSTYPE, CLM_SRC_PROV_CLM_BIL_NPI_ID, CLM_SRC_PROV_CLM_REF_NPI_ID,
 CLM_SRC_PROV_CLM_RNDR_NPI_ID, CLM_AUTH_NBR
FROM TEMP_jn_stg 
WHERE (CRR_CLM_HEAD_SYS_ID <> -1 or CRR_CLM_HEAD_SYS_ID <> 0) ) ; 



CREATE OR REPLACE TEMPORARY TABLE TEMP_extLVL_OF_CARE_XREF AS
(SELECT trim(CARE_XREF.PROC_TYP_CD) AS PROC_TYP_CD,
 trim(CARE_XREF.PROC_CD) AS PROC_CD,
 trim(coalesce(CARE_XREF.LVL_OF_CARE_DERIV_TXT,'''')) AS LVL_OF_CARE_DERIV_TXT,
 trim(coalesce(CARE_XREF.LVL_OF_SRVC_DERIV_CD,'''')) AS LVL_OF_SRVC_DERIV_CD,
 trim(coalesce(CARE_XREF.UNIT_TYP_CD,'''')) AS UNIT_TYP_CD, 
 ''1'' AS CLM_REC_EXISTS 
FROM COMPACT.TB_SYS_DERIV_LVL_OF_CARE_XREF  CARE_XREF ) ;



CREATE OR REPLACE TEMPORARY TABLE TEMP_lkp_DIAG AS
(SELECT A.*, B.LVL_OF_CARE_DERIV_TXT, B.LVL_OF_SRVC_DERIV_CD, B.UNIT_TYP_CD, B.CLM_REC_EXISTS
FROM TEMP_t1xfrm_MAP_2 A LEFT OUTER JOIN TEMP_extLVL_OF_CARE_XREF B ON 
A.PROC_TYP_CD = B.PROC_TYP_CD AND A.DLOC_PROC_CD = B.PROC_CD
) ;


CREATE OR REPLACE TEMPORARY TABLE TEMP_t5xfrm_SRVC AS
(SELECT 
PROC_TYP_CD,
DLOC_PROC_CD,
CASE WHEN DLOC_PROC_CD = ''90899'' Then 1 Else CLM_REC_EXISTS END AS CLM_REC_EXISTS,
 
 CASE WHEN DLOC_PROC_CD = ''90899'' Then split_part(rClaimsDLOCDerivInputs_CRR(Trim(DLOC_PROC_CD),Trim(POSCODE)), ''#'', 1) Else LVL_OF_CARE_DERIV_TXT END AS CLM_LVL_OF_CARE_DERIV_TXT,
 CASE WHEN DLOC_PROC_CD = ''90899'' Then split_part(rClaimsDLOCDerivInputs_CRR(Trim(DLOC_PROC_CD),Trim(POSCODE)), ''#'', 2) Else LVL_OF_SRVC_DERIV_CD end as CLM_LVL_OF_SRVC_DERIV_CD,
 CASE WHEN DLOC_PROC_CD = ''90899'' Then split_part(rClaimsDLOCDerivInputs_CRR(Trim(DLOC_PROC_CD),Trim(POSCODE)), ''#'', 3) Else UNIT_TYP_CD END AS CLM_UNIT_TYP_CD,
 
-- '''' AS CLM_LVL_OF_CARE_DERIV_TXT,
-- '''' AS CLM_LVL_OF_SRVC_DERIV_CD,
-- '''' AS CLM_UNIT_TYP_CD,
 
AUTH_SYS_ID,
AUTH_LVL_OF_SRVC_CD,
AUTH_LVL_OF_CARE_TXT,
AUTH_NBR,
PROC_CPT_TYP_CD,
UNIQ_CLM_AUD_NBR,
CLM_AUD_LN_NBR,
CRR_CLM_SRVC_SYS_ID,
CLAIMID,
BTCH_LOAD_DT,
CLM_SRVC_ADJ_CD,
CLM_SRVC_CLM_STS_CD,
SRVC_STRT_DT,
SRVC_END_DT,
CLM_SRVC_PD_DT,
CLM_SRVC_PST_DT,
CLM_SRVC_ADJD_DT,
ALLOWEDAMT,
COBADJUSTMENTAMT,
MEMCOINSURANCEREDUCTAMT,
COPAYREDUCTAMT,
DEDUCTIBLEREDUCTAMT,
PROMPTPAYDISCOUNTREDUCTAMT,
BILLEDANDTAXAMT,
PRICEDAMT,
NONCOVEREDAMT,
FINALMEMRESPAMT,
FINALNETPAYAMT,
CLM_SRVC_UNIT_CNT,
CLM_SRVC_UNIT_DERIV_CNT,
CLM_SRVC_DIAG_ADMIT_CD,
CLM_SRVC_DIAG_1_CD,
CLM_SRVC_DIAG_2_CD,
CLM_SRVC_DIAG_3_CD,
CLM_SRVC_DIAG_4_CD,
CLM_SRVC_DIAG_5_CD,
CLM_SRVC_DIAG_6_CD,
CLM_SRVC_DIAG_7_CD,
CLM_SRVC_DIAG_8_CD,
CLM_SRVC_DIAG_9_CD,
CLM_SRVC_DIAG_10_CD,
CLM_SRVC_PROC_MOD_CD,
CLM_SRVC_PROC_MOD_2_CD,
CLM_SRVC_PROC_MOD_3_CD,
CLM_SRVC_PROC_MOD_4_CD,
MH_SA_CD_1,
MH_SA_CD_2,
MH_SA_CD_3,
MH_SA_CD_4,
LINEOFBUSINESSID,
CLM_MBR_SRC_MBR_SYS_ID,
CHECKNUM,
PAYEEENTITYTYPE,
CLM_SRVC_PROC_CD,
ICD_DIAG_TYP_CD,
CLM_MBR_ALT_ID,
SRC_REC_CRT_DTTM,
SRC_REC_UPDT_DTTM,
CRR_CLM_HEAD_SYS_ID,
CLM_TYP_CD,
CLM_FM_TYP_CD,
RCRC_ID,
IPCD_ID,
PROV_SYS_ID,
PROC_CD,
POSCODE,
REVENUECODE,
SRC_FINC_LIAB_CD,
DERIV_OBH_FINC_LIAB,
SRC_CRR_PROV_ID,
PROVIDERID,
PROV_ADR_CITY_NM,
PROV_ADR_ST_CD,
PROV_ADR_ZIP_CD,
PROV_NPI_ID,
PROV_TAX_ID,
PROV_CHK_NM,
PYMT_EOB_RSN_CD,
PYMT_EOB_RSN_CD_DESC,
PROVIDERPARSTATUSTYPE,
BENEFITTIERPARSTATUSTYPE,
CLM_SRC_PROV_CLM_BIL_NPI_ID,
CLM_SRC_PROV_CLM_REF_NPI_ID,
CLM_SRC_PROV_CLM_RNDR_NPI_ID,
CLM_AUTH_NBR
FROM TEMP_lkp_DIAG
) ;



CREATE OR REPLACE TEMPORARY TABLE TEMP_extCARE_CATGY_XREF AS
(SELECT 
 trim(LVL_OF_CARE_DERIV_TXT) as LVL_OF_CARE_DERIV_TXT,
  LVL_OF_CARE_SEQ_NBR as CLM_LVL_OF_CARE_SEQ_NBR
FROM COMPACT.TB_SYS_DERIV_LVL_OF_CARE_CATGY_XREF) ;


CREATE OR REPLACE TEMPORARY TABLE TEMP_extAUTHCARE_CATGY_XREF AS
(SELECT 
 trim(LVL_OF_CARE_DERIV_TXT) as LVL_OF_CARE_DERIV_TXT,
  LVL_OF_CARE_SEQ_NBR as AUTH_LVL_OF_CARE_SEQ_NBR
FROM COMPACT.TB_SYS_DERIV_LVL_OF_CARE_CATGY_XREF) ;


CREATE OR REPLACE TEMPORARY TABLE TEMP_lkpCatgy AS
(SELECT A.*, B.CLM_LVL_OF_CARE_SEQ_NBR, C.AUTH_LVL_OF_CARE_SEQ_NBR
FROM TEMP_t5xfrm_SRVC A LEFT OUTER JOIN TEMP_extCARE_CATGY_XREF B ON A.CLM_LVL_OF_CARE_DERIV_TXT= B.LVL_OF_CARE_DERIV_TXT 
LEFT OUTER JOIN TEMP_extAUTHCARE_CATGY_XREF C ON A.AUTH_LVL_OF_CARE_TXT = C.LVL_OF_CARE_DERIV_TXT ) ;


CREATE OR REPLACE TEMPORARY TABLE TEMP_lCat12 AS
(SELECT A.*, 
 
rClaimsDLOC_CRR(PROC_TYP_CD,Trim(DLOC_PROC_CD),Trim(CLM_SRVC_UNIT_CNT), '''','''',Trim(CLM_UNIT_TYP_CD), CASE WHEN CLM_REC_EXISTS=1  Then ''Y'' Else ''N'' END,Trim(CLM_LVL_OF_CARE_DERIV_TXT),Trim(CLM_LVL_OF_SRVC_DERIV_CD),CASE WHEN AUTH_SYS_ID >-1  Then ''Y'' Else ''N'' END,trim(AUTH_LVL_OF_CARE_TXT),trim(AUTH_LVL_OF_SRVC_CD),CLM_LVL_OF_CARE_SEQ_NBR,AUTH_LVL_OF_CARE_SEQ_NBR,
CASE WHEN (CASE WHEN (CASE WHEN CLM_REC_EXISTS=1  Then ''Y'' Else ''N'' END)=''Y'' And (trim(CLM_LVL_OF_CARE_DERIV_TXT) = ''Partial Hospitalization'' or trim(CLM_LVL_OF_CARE_DERIV_TXT) = ''Day Treatment'' or trim(CLM_LVL_OF_CARE_DERIV_TXT) =  ''Structured Outpatient'' or trim(CLM_LVL_OF_CARE_DERIV_TXT) = ''Residential'' or trim(CLM_LVL_OF_CARE_DERIV_TXT) = ''Recovery Home'') Then 1 Else 0 END)=1 And (CASE WHEN (CASE WHEN AUTH_SYS_ID >-1  Then ''Y'' Else ''N'' END)=''Y'' And (trim(AUTH_LVL_OF_CARE_TXT) = ''Partial Hospitalization'' or trim(AUTH_LVL_OF_CARE_TXT) = ''Structured Outpatient'' or trim(AUTH_LVL_OF_CARE_TXT) = ''Residential'' or trim(AUTH_LVL_OF_CARE_TXT) = ''Recovery Home'') Then 1 Else 0 END)=1 Then ''Y'' Else ''N'' END) AS svDLOC 

 FROM TEMP_lkpCatgy A
);



CREATE OR REPLACE TEMPORARY TABLE TEMP_Tfm_Src_To_tgt AS
(SELECT 
CRR_CLM_SRVC_SYS_ID,
CRR_CLM_HEAD_SYS_ID,
CLAIMID AS CLM_AUD_NBR,
Right(''000'' || CLM_AUD_LN_NBR,3) AS CLM_AUD_LN_NBR,
PROV_SYS_ID,
SRC_CRR_PROV_ID,
CLM_SRVC_ADJ_CD,
CLM_SRVC_CLM_STS_CD,
SRVC_STRT_DT AS CLM_SRVC_FROM_DT,
SRVC_END_DT AS CLM_SRVC_THRU_DT,
CLM_SRVC_PD_DT,
CLM_SRVC_PST_DT,
CLM_SRVC_ADJD_DT,
ALLOWEDAMT AS AMT_ALLW_AMT,
NVL(FINALNETPAYAMT,0) + NVL(COPAYREDUCTAMT,0) + NVL(MEMCOINSURANCEREDUCTAMT,0) + NVL(DEDUCTIBLEREDUCTAMT,0)  + NVL(COBADJUSTMENTAMT,0) AS AMT_ALLW_DERIV_AMT,
COBADJUSTMENTAMT AS AMT_COB_AMT,
MEMCOINSURANCEREDUCTAMT AS AMT_COINS_AMT,
PRICEDAMT AS AMT_CONTR_AMT,
COPAYREDUCTAMT AS AMT_COPAY_AMT,
DEDUCTIBLEREDUCTAMT AS AMT_DED_AMT,
NONCOVEREDAMT AS AMT_DSALLW_AMT,
PROMPTPAYDISCOUNTREDUCTAMT AS AMT_DSCNT_AMT,
FINALMEMRESPAMT AS AMT_MBR_RESP_AMT,
FINALNETPAYAMT AS AMT_PD_AMT,
0 AS AMT_RSRV_AMT,
BILLEDANDTAXAMT AS AMT_SBMT_AMT,
CLM_SRVC_UNIT_CNT,

SPLIT_PART(svDLOC, ''#'', 1) AS LVL_OF_CARE_DERIV_TXT,

CASE WHEN SPLIT_PART(svDLOC, ''#'', 2) = ''ANC'' or SPLIT_PART(svDLOC, ''#'', 2) = ''ECT'' or SPLIT_PART(svDLOC, ''#'', 2) = ''TRA''  or SPLIT_PART(svDLOC, ''#'', 2) = ''ER'' or SPLIT_PART(svDLOC, ''#'', 2) = ''HCB'' or SPLIT_PART(svDLOC, ''#'', 2) = ''LAB'' 
or SPLIT_PART(svDLOC, ''#'', 2) = ''RX'' or SPLIT_PART(svDLOC, ''#'', 2) = ''TMS'' Then SPLIT_PART(svDLOC, ''#'', 2) WHEN Len(trim(MH_SA_CD_1)) = 0 Then ''U'' || SUBSTR(SPLIT_PART(svDLOC, ''#'', 2),2,2) WHEN Trim(MH_SA_CD_1) = ''SA'' Then ''C'' || SUBSTR(SPLIT_PART(svDLOC, ''#'', 2),2,2) Else ''P'' || SUBSTR(SPLIT_PART(svDLOC, ''#'', 2),2,2) END AS LVL_OF_SRVC_DERIV_CD,

''PAYBL'' AS LVL_OF_SRVC_SRC_CD,
CASE WHEN (PROC_CPT_TYP_CD=''AO'' or PROC_CPT_TYP_CD=''AO/MD'') Then 0 WHEN SPLIT_PART(svDLOC, ''#'', 1) = ''Ancillary'' And CLM_SRVC_UNIT_DERIV_CNT > 0 Then 1 Else CASE WHEN SPLIT_PART(svDLOC, ''#'', 3)='''' THEN 0 ELSE SPLIT_PART(svDLOC, ''#'', 3) END  END AS CLM_SRVC_UNIT_DERIV_CNT,
//CASE WHEN (PROC_CPT_TYP_CD=''AO'' or PROC_CPT_TYP_CD=''AO/MD'') Then 0 WHEN SPLIT_PART(svDLOC, ''#'', 1) = ''Ancillary'' And CLM_SRVC_UNIT_DERIV_CNT > 0 Then 1 Else 0 END AS CLM_SRVC_UNIT_DERIV_CNT,

 CLM_UNIT_TYP_CD AS CLM_SRVC_UNIT_OF_MSR_DESC,
'''' AS CLM_SRVC_ACCOM_FLG,
'''' AS CLM_SRVC_RM_TYP_CD,
'''' AS CLM_SRVC_RM_TYP_DESC,
CLM_SRVC_DIAG_ADMIT_CD,
CLM_SRVC_DIAG_1_CD,
CLM_SRVC_DIAG_2_CD,
CLM_SRVC_DIAG_3_CD,
CLM_SRVC_DIAG_4_CD,
CLM_SRVC_DIAG_5_CD,
CLM_SRVC_DIAG_6_CD,
CLM_SRVC_DIAG_7_CD,
CLM_SRVC_DIAG_8_CD,
CLM_SRVC_DIAG_9_CD,
CLM_SRVC_DIAG_10_CD,
CASE WHEN trim(MH_SA_CD_1)= ''SA'' or trim(MH_SA_CD_2)=''SA'' or trim(MH_SA_CD_3)=''SA'' or trim(MH_SA_CD_4)=''SA'' then ''Y'' else ''N'' END AS DIAG_SBSTNC_ABUS_FLG,
TRIM(POSCODE) AS PL_OF_SRVC_AMA_CD,
trim(REVENUECODE) AS CLM_SRVC_RVNU_CD,
NVL(CLM_SRVC_PROC_MOD_CD,'''') AS CLM_SRVC_PROC_MOD_CD,
NVL(CLM_SRVC_PROC_MOD_2_CD,'''') AS CLM_SRVC_PROC_MOD_2_CD,
NVL(CLM_SRVC_PROC_MOD_3_CD,'''') AS CLM_SRVC_PROC_MOD_3_CD,
NVL(CLM_SRVC_PROC_MOD_4_CD,'''') AS CLM_SRVC_PROC_MOD_4_CD,
PROVIDERID AS CLM_SRVC_SRC_PROV_ID,
PROVIDERPARSTATUSTYPE AS CLM_SRVC_PROV_IN_NTWK_CD,
trim(PROV_ADR_CITY_NM) AS PROV_SRVC_CTY_NM,
trim(PROV_ADR_ST_CD) AS PROV_SRVC_ST_CD,
trim(PROV_ADR_ZIP_CD) AS PROV_SRVC_ZIP_CD,
PROV_NPI_ID,
PROV_TAX_ID,
BENEFITTIERPARSTATUSTYPE AS CLM_SRVC_BEN_LVL_DERIV_CD,
'''' AS CLM_SRVC_BEN_LVL_SRC_CD,
PYMT_EOB_RSN_CD,
PYMT_EOB_RSN_CD_DESC,
LINEOFBUSINESSID AS CLM_SRVC_LN_OF_BUS_ID,
CLM_MBR_SRC_MBR_SYS_ID AS CLM_SRVC_SRC_MBR_ID,
'''' AS PYMT_OVRIDE_CD,
CHECKNUM AS CLM_CHK_NBR,
CLM_AUTH_NBR,
''-1'' AS AUTH_SYS_ID,
PROV_CHK_NM AS PROV_PAYE_NM,
CASE WHEN trim(PAYEEENTITYTYPE)=7  then ''F'' else ''V'' END AS PRI_PAYE_CD,
CASE WHEN trim(PAYEEENTITYTYPE)=7  then ''Family'' else ''Vendor'' END AS PRI_PAYE_CD_DESC,
CLM_SRVC_PROC_CD,
ICD_DIAG_TYP_CD,
PROC_CD as DERIV_PROC_CD,
CLM_MBR_ALT_ID,
SRC_REC_CRT_DTTM,
SRC_REC_UPDT_DTTM,
CURRENT_TIMEstamp() AS REC_CRT_DTTM,
CURRENT_TIMESTAMP() AS REC_UPDT_DTTM,
''CRR'' AS REC_XTRCT_SRC_CD,
''CRR'' AS REC_INPUT_SRC_CD,
BTCH_LOAD_DT,
CLM_TYP_CD,
SRC_FINC_LIAB_CD,
DERIV_OBH_FINC_LIAB,
CLM_SRC_PROV_CLM_BIL_NPI_ID,
CLM_SRC_PROV_CLM_REF_NPI_ID,
CLM_SRC_PROV_CLM_RNDR_NPI_ID
FROM TEMP_lCat12
) ;


CREATE OR REPLACE TRANSIENT TABLE COMPACT.TB_CLM_CRR_CLM_SRVC AS
(SELECT 
CRR_CLM_SRVC_SYS_ID,
CRR_CLM_HEAD_SYS_ID,
CLM_AUD_NBR,
CLM_AUD_LN_NBR,
PROV_SYS_ID,
SRC_CRR_PROV_ID,
CLM_SRVC_ADJ_CD,
CLM_SRVC_CLM_STS_CD,
CLM_SRVC_FROM_DT,
CLM_SRVC_THRU_DT,
CLM_SRVC_PD_DT,
CLM_SRVC_PST_DT,
CLM_SRVC_ADJD_DT,
CASE WHEN AMT_ALLW_AMT > ''9999999.99'' Then 0 Else NVL(AMT_ALLW_AMT,0) END AS  AMT_ALLW_AMT,
CASE WHEN AMT_ALLW_DERIV_AMT > ''9999999.99'' Then 0 Else NVL(AMT_ALLW_DERIV_AMT,0) END AS AMT_ALLW_DERIV_AMT,
CASE WHEN AMT_COB_AMT > ''9999999.99'' Then 0 Else NVL(AMT_COB_AMT,0) END AS AMT_COB_AMT,
CASE WHEN AMT_COINS_AMT > ''9999999.99'' Then 0 Else NVL(AMT_COINS_AMT,0) END AS AMT_COINS_AMT,
CASE WHEN AMT_CONTR_AMT > ''9999999.99'' Then 0 Else NVL(AMT_CONTR_AMT,0) END AS AMT_CONTR_AMT,
CASE WHEN AMT_COPAY_AMT > ''9999999.99'' Then 0 Else NVL(AMT_COPAY_AMT,0) END AS AMT_COPAY_AMT,
CASE WHEN AMT_DED_AMT > ''9999999.99'' Then 0 Else NVL(AMT_DED_AMT,0) END AS AMT_DED_AMT,
CASE WHEN AMT_DSALLW_AMT > ''9999999.99'' Then 0 Else NVL(AMT_DSALLW_AMT,0) END AS AMT_DSALLW_AMT,
CASE WHEN AMT_DSCNT_AMT > ''9999999.99'' Then 0 Else NVL(AMT_DSCNT_AMT,0) END AS AMT_DSCNT_AMT,
CASE WHEN AMT_MBR_RESP_AMT > ''9999999.99'' Then 0 Else NVL(AMT_MBR_RESP_AMT,0) END AS AMT_MBR_RESP_AMT,
CASE WHEN AMT_PD_AMT > ''9999999.99'' Then 0 Else NVL(AMT_PD_AMT,0) END AS AMT_PD_AMT,
CASE WHEN AMT_RSRV_AMT > ''9999999.99'' Then 0 Else NVL(AMT_RSRV_AMT,0) END AS AMT_RSRV_AMT,
CASE WHEN AMT_SBMT_AMT > ''9999999.99'' Then 0 Else NVL(AMT_SBMT_AMT,0) END AS AMT_SBMT_AMT,
CLM_SRVC_UNIT_CNT,
LVL_OF_CARE_DERIV_TXT,
LVL_OF_SRVC_DERIV_CD,
LVL_OF_SRVC_SRC_CD,
CLM_SRVC_UNIT_DERIV_CNT,
CLM_SRVC_UNIT_OF_MSR_DESC,
CLM_SRVC_ACCOM_FLG,
CLM_SRVC_RM_TYP_CD,
CLM_SRVC_RM_TYP_DESC,
CLM_SRVC_DIAG_ADMIT_CD,
CLM_SRVC_DIAG_1_CD,
CLM_SRVC_DIAG_2_CD,
CLM_SRVC_DIAG_3_CD,
CLM_SRVC_DIAG_4_CD,
CLM_SRVC_DIAG_5_CD,
CLM_SRVC_DIAG_6_CD,
CLM_SRVC_DIAG_7_CD,
CLM_SRVC_DIAG_8_CD,
CLM_SRVC_DIAG_9_CD,
CLM_SRVC_DIAG_10_CD,
DIAG_SBSTNC_ABUS_FLG,
PL_OF_SRVC_AMA_CD,
CLM_SRVC_RVNU_CD,
CLM_SRVC_PROC_MOD_CD,
CLM_SRVC_PROC_MOD_2_CD,
CLM_SRVC_PROC_MOD_3_CD,
CLM_SRVC_PROC_MOD_4_CD,
CLM_SRVC_SRC_PROV_ID,
CLM_SRVC_PROV_IN_NTWK_CD,
PROV_SRVC_CTY_NM,
PROV_SRVC_ST_CD,
PROV_SRVC_ZIP_CD,
PROV_NPI_ID,
PROV_TAX_ID,
CLM_SRVC_BEN_LVL_DERIV_CD,
CLM_SRVC_BEN_LVL_SRC_CD,
PYMT_EOB_RSN_CD,
PYMT_EOB_RSN_CD_DESC,
CLM_SRVC_LN_OF_BUS_ID,
CLM_SRVC_SRC_MBR_ID,
PYMT_OVRIDE_CD,
CLM_CHK_NBR,
CLM_AUTH_NBR,
AUTH_SYS_ID,
PROV_PAYE_NM,
PRI_PAYE_CD,
PRI_PAYE_CD_DESC,
CLM_SRVC_PROC_CD,
ICD_DIAG_TYP_CD,
DERIV_PROC_CD,
CLM_MBR_ALT_ID,
SRC_REC_CRT_DTTM,
SRC_REC_UPDT_DTTM,
REC_CRT_DTTM,
REC_UPDT_DTTM,
REC_XTRCT_SRC_CD,
REC_INPUT_SRC_CD,
BTCH_LOAD_DT,
SRC_FINC_LIAB_CD,
DERIV_OBH_FINC_LIAB,
CLM_SRC_PROV_CLM_BIL_NPI_ID,
CLM_SRC_PROV_CLM_REF_NPI_ID,
CLM_SRC_PROV_CLM_RNDR_NPI_ID FROM TEMP_Tfm_Src_To_tgt) ;


SELECT COUNT(1) INTO ins_cnt FROM COMPACT.TB_CLM_CRR_CLM_SRVC;

 SELECT CURRENT_TIMESTAMP() INTO load_end_ts ;
 INSERT INTO ETL.DBA_PROCESS_LOG VALUES
 (:unique_id,''TB_CLM_CRR_CLM_SRVC'',''TB_CLM_CRR_CLM_SRVC'',''LOAD'',0,0,:load_strt_ts,:load_end_ts,
 CURRENT_TIMESTAMP,:ins_cnt,0,0) ;
 result:=''TB_CLM_CRR_CLM_SRVC refresh completed successfully'';
 return result;
 end;
  ';