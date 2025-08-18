CREATE OR REPLACE PROCEDURE NORTHSTAR.NS_MBR_LOAD_FL(START_DATE VARCHAR, END_DATE VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try
    {

   SQLcmdMAX = "SELECT distinct max(to_char(memb_demographics_full_date)) FROM compact.fact_member_demographics";
      var sqlmaxdt = snowflake.createStatement({sqlText: SQLcmdMAX});
      var resultCountmaxdt = sqlmaxdt.execute();
      resultCountmaxdt.next();
      max_date = resultCountmaxdt.getColumnValue(1);


             SQLcmd ="INSERT INTO NORTHSTAR.NS_MEMBER   select   SUBSTR(COMPANY_CODE,4,2) STATE_CODE,FMD.MEMB_DIM_ID as SRC_MBR_ID,subscriber_id,MEMB_FIRST_NAME,MEMB_LAST_NAME,DOB,MBR.MEMB_ADDRESS_LINE_1,MBR.MEMB_CITY,MBR.MEMB_STATE,MBR.MEMB_ZIP,LOB.LINE_OF_BUSINESS_DESC LOB1,PLAN_CODE BX_DX,PLAN_DESC DX_DESC,MEDICAID_NO,null BH_COST_OF_CARE,  null MED_COST_OF_CARE, null  RX_COST_OF_CARE, null TOTAL_COST_OF_CARE,  null SEGMENT1,'Y' IS_NEW,'Y' IS_ACTIVE,current_date() REC_CRT_DTTM,null REC_UPDT_DTTM,null AE_NM,null EXTRACT_START_DT,null EXTRACT_END_DT,null,null,null,MBR.SEQ_MEMB_ID SEQ_MEMB_ID1,MBR.DOD,MBR.GENDER,MBR.SOCIAL_SEC_NO,null COUNTY from compact.fact_member_demographics  FMD inner join COMPACT.DIM_MEMBER   MBR on  FMD.MEMB_DIM_ID = MBR.MEMB_DIM_ID   and FMD.memb_demographics_full_date >= to_date(:1) and datediff(year,MBR.DOB,to_date(:1))>= (select distinct age FROM  NORTHSTAR.CONFIG_LMT_LOBS where state='FL')  and MBR.MCARE_DUAL_ENROLLED_FLAG='N' and mbr.dod is null and  MBR.MEMB_DIM_ID in  (select  distinct m.MEMB_DIM_ID from compact.dim_member m where memb_elig_status = 'Y'and exists (select 1 from compact.fact_member_elig_history h where h.memb_dim_id = m.memb_dim_id  and h.term_date > current_date)) INNER JOIN (select * from COMPACT.DIM_PLAN PLN where plan_code in (select distinct PLAN_IPRO   from NORTHSTAR.CONFIG_LMT_LOBS where state='FL') ) PLN ON PLN.PLAN_DIM_ID=FMD.PLAN_DIM_ID  inner join COMPACT.DIM_COMPANY CMP on CMP.COMPANY_DIM_ID=FMD.COMPANY_DIM_ID   inner join COMPACT.DIM_LINE_OF_BUSINESS LOB on LOB.LINE_OF_BUSINESS_DIM_ID=FMD.LINE_OF_BUSINESS_DIM_ID qualify row_number() over (partition by FMD.MEMB_DIM_ID order by FMD.MEMB_DIM_ID asc) = 1 order by FMD.MEMB_DIM_ID " ;
             var stmt = snowflake.createStatement({sqlText: SQLcmd, binds: [max_date]});
       var result = stmt.execute();
        result.next();
        resultSet = result.getColumnValue(1);

		 SQLcmdNSCountyupd= "update NORTHSTAR.NS_MEMBER a set a.county=b.county from COMPACT.DIM_MEMBER_ADDRESS b where src_mbr_id=b.memb_dim_id and addr_type='H' and state_code='FL'";
      var stmtNSCountyupd = snowflake.createStatement({ sqlText: SQLcmdNSCountyupd} );
       var resultNSCountyupd = stmtNSCountyupd.execute();
       resultNSCountyupd.next();
       var resultNSCountyupd = resultNSCountyupd.getColumnValue(1);

		SQLcmdFullRisk ="CREATE OR REPLACE temporary table NORTHSTAR.AE_ATTR  as (SELECT  distinct state_code,fmd.memb_dim_id,Type ||' - ' || VEND_FULL_NAME ||' - ' || contract_entity AE FROM   compact.fact_member_demographics fmd inner join NORTHSTAR.NS_MEMBER on src_mbr_id=fmd.memb_dim_id and state_code='FL' INNER JOIN compact.dim_provider dprov ON fmd.prov_dim_id = dprov.prov_dim_id AND dprov.company_dim_id IN ( 589 ) INNER JOIN compact.dim_vendor dvend ON dprov.dflt_vend_dim_id = dvend.vend_dim_id  AND dvend.company_dim_id IN ( 589 ) inner join NORTHSTAR.DIM_VBP_CONTRACTS_FIN on irs_tax_id=taxid LEFT JOIN COMPACT.DIM_PLAN_LOB DPL ON DPL.PLAN_LOB_DIM_ID = FMD.PLAN_LOB_DIM_ID LEFT JOIN COMPACT.DIM_PLAN_PROGRAM DPP ON DPP.PLAN_PROGRAM_DIM_ID = DPL.PLAN_PROGRAM_DIM_ID LEFT JOIN COMPACT.DIM_PLAN_PROGRAM_TYPE DPPT ON DPP.PLAN_PROGRAM_TYPE_DIM_ID = DPPT.PLAN_PROGRAM_TYPE_DIM_ID WHERE   memb_demographics_full_date = to_date(:1) and COALESCE (CASE WHEN DPPT.MEDICARE_DUAL_FLAG_DIM_ID = 2 THEN 'DUAL'ELSE COALESCE(NULLIF(DPP.PROGRAM_TYPE_CODE, 'N/A'), 'TANF')  END, '')  not in ('DUAL','LTCCP','LTCCM') ) ";
             var stmtFullRisk = snowflake.createStatement({sqlText: SQLcmdFullRisk, binds: [max_date]});
       var resultFullRisk = stmtFullRisk.execute();
        resultFullRisk.next();
        resultSetFullRisk = resultFullRisk.getColumnValue(1);

        SQLcmdFullRiskupd="UPDATE NORTHSTAR.NS_MEMBER t1 SET t1.AE_NM=t2.AE FROM NORTHSTAR.AE_ATTR t2 WHERE t1.src_mbr_id=t2.memb_dim_id and AE_NM is null and t1.STATE_CODE=t2.STATE_code and t1.state_code='FL'";
        var stmtFullRiskupd = snowflake.createStatement({ sqlText: SQLcmdFullRiskupd} );
        var resultFullRiskupd = stmtFullRiskupd.execute();
        resultFullRiskupd.next();
        var resultFullRiskupd = resultFullRiskupd.getColumnValue(1);

			sqlcmdState= "select distinct state_code from NORTHSTAR.NS_MEMBER where state_code  in ('FL') order by state_code";
		    var sqlState = snowflake.createStatement({sqlText: sqlcmdState});
		    var resultState = sqlState.execute();
		    while ( resultState.next()) {
			var State1= resultState.getColumnValue(1);
		     SQLcmdMAX = "select state||Start_dt||end_dt from (select *, substr(p1,5,2) pp1_month, substr(p1,1,4) pp1_year, substr(p2,5,2) pp2_month, substr(p2,1,4) pp2_year,to_char(pp1_year||'-'||pp1_month||'-01') start_dt,to_char(last_day(to_date(to_char(pp2_year||'-'||pp2_month||'-01')),month))end_dt from (select distinct state,period,split_part(period,'-',1) p1,split_part(period,'-',2) p2 from (select state,max(period) Period from COMPACT.ACO_SNAPSHOT_SUMMARY where state=:1 group by state)))";
		       var sqlmaxdt = snowflake.createStatement({sqlText: SQLcmdMAX, binds: [State1]});
		      var resultCountmaxdt = sqlmaxdt.execute();
		      resultCountmaxdt.next();
		      max_date1 = resultCountmaxdt.getColumnValue(1);

		     SQLcmdACO="create or replace temporary table NORTHSTAR.AE_ATTR  as (SELECT   P.Memb_Dim_Id,p.state, first_value(R.CONTRACT_PROGRAM) over (partition by P.Memb_Dim_Id order by R.CONTRACT_END_DATE desc)  ||' - ' ||first_value(R.GROUP_NAME) over (partition by P.Memb_Dim_Id order by R.CONTRACT_END_DATE desc)  AE,to_date(substr(:1,3,10)) START_DT, to_date(substr(:1,13,10)) END_DT,substr(:1,1,2) st  FROM COMPACT.ACO_SNAPSHOT_SUMMARY P  INNER JOIN NORTHSTAR.NS_MEMBER ON MEMB_DIM_ID=SRC_MBR_ID AND STATE_CODE=P.STATE and state_code  in ('FL') and AE_NM is null and  P.state=substr(:1,1,2)  Right Join COMPACT.ACO_tin_List Q     On (P.Irs_Tax_Id=Q.TIN)             AND P.state=Q.state   AND Q.TIN_END_DATE>= to_date(dateadd(day,1,substr(:1,13,10)))       AND Q.Tin_Status  = 'Active' Right Join COMPACT.ACO_Contracts R                                          On (R.ACCOUNT_ID=Q.ACCOUNT_ID)       AND R.State=Q.State       AND R.Contract_Status = 'Active'      AND R.program_desc not in ('PMPM_BQM')  Right JOIN COMPACT.ACO_PRODUCTS T                                                      ON T.Account_ID = R.Account_id      AND T.Line_of_Business = P.LINE_OF_BUSINESS      AND T.PROGRAM_TYPE_DESC = P.PROGRAM_TYPE_DESC      AND UPPER(T.STATUS) = 'INCLUDE'    WHERE  PERIOD IN (year(START_DT)||lpad (month(START_DT),2,0)||'-'||year(END_DT)||lpad (month(END_DT),2,0)) )";
		      var stmtACO = snowflake.createStatement({ sqlText: SQLcmdACO, binds: [max_date1]} );
		      var resultACO = stmtACO.execute();
		      resultACO.next();
		      var resultACO = resultACO.getColumnValue(1);
              SQLcmdACOupd="UPDATE NORTHSTAR.NS_MEMBER t1 SET t1.AE_NM=t2.AE FROM NORTHSTAR.AE_ATTR t2 WHERE t1.src_mbr_id=t2.memb_dim_id and AE_NM is null and STATE_CODE=t2.STATE and state_code='FL'";
              var stmtACOupd = snowflake.createStatement({ sqlText: SQLcmdACOupd, binds: [START_DATE, END_DATE]} );
              var resultACOupd = stmtACOupd.execute();
              resultACOupd.next();
              var resultACOupd = resultACOupd.getColumnValue(1);

		     }

                SQLcmdFSP="CREATE OR REPLACE TEMPORARY TABLE NORTHSTAR.AE_ATTR AS (select memb_dim_id,'FSP' as desc from (select * from NORTHSTAR.NS_MEMBER NS inner join COMPACT.DIM_MEMBER  MBR ON MBR.MEMB_DIM_ID= NS.SRC_MBR_ID AND STATE_CODE='FL' AND    datediff(YEAR, MBR.DOB, :2) <=  (select distinct AGE from NORTHSTAR.CONFIG_AE_AGE where AE_NM='FSP'))MBR    )";
                var stmtFSP = snowflake.createStatement({ sqlText: SQLcmdFSP , binds: [START_DATE, END_DATE]} );
                var resultFSP = stmtFSP.execute();
                resultFSP.next();
                var resultFSP = resultFSP.getColumnValue(1);

                SQLcmdFSPupd="UPDATE NORTHSTAR.NS_MEMBER t1 SET t1.AE_NM='FSP' FROM NORTHSTAR.AE_ATTR t2 WHERE t1.src_mbr_id=t2.memb_dim_id and AE_NM is null AND t1.STATE_CODE='FL' ";
                var stmtFSPupd = snowflake.createStatement({ sqlText: SQLcmdFSPupd , binds: [START_DATE, END_DATE]} );
                var resultFSPupd = stmtFSPupd.execute();
                resultFSPupd.next();
                var resultFSPupd = resultFSPupd.getColumnValue(1);

               SQLcmdCNupd="UPDATE NORTHSTAR.NS_MEMBER t1 SET t1.AE_NM='CARE NAVIGATION'  WHERE  AE_NM is null AND STATE_CODE='FL' ";
                var stmtCNupd = snowflake.createStatement({ sqlText: SQLcmdCNupd , binds: [START_DATE, END_DATE]} );
                 var resultCNupd = stmtCNupd.execute();
                 resultCNupd.next();
                 var resultCNupd = resultCNupd.getColumnValue(1);

			   SQLcmdAmtCalc="create or replace temporary table NORTHSTAR.NS_AMT_FL as (   select distinct src_mbr_id,sum(BH_COST_OF_CARE1) OVER (PARTITION BY SRC_MBR_ID order by SRC_MBR_ID)   BH_COST_OF_CARE, sum(MED_COST_OF_CARE1) OVER (PARTITION BY SRC_MBR_ID order by SRC_MBR_ID)  MED_COST_OF_CARE, sum(RX_COST_OF_CARE1) OVER (PARTITION BY SRC_MBR_ID order by SRC_MBR_ID)    RX_COST_OF_CARE,   TOTAL_COST_OF_CARE  from (  select distinct src_mbr_id, case when CLAIM_TYPE_DIM_ID IN (6,7,8) then sum(PAID_NET_AMT) OVER (PARTITION BY SRC_MBR_ID,CLAIM_TYPE_DIM_ID order by SRC_MBR_ID) else 0  end BH_COST_OF_CARE1,  case when CLAIM_TYPE_DIM_ID IN (1,2) then sum(PAID_NET_AMT) OVER (PARTITION BY SRC_MBR_ID,CLAIM_TYPE_DIM_ID order by SRC_MBR_ID)  else 0 end MED_COST_OF_CARE1, case when CLAIM_TYPE_DIM_ID IN (3) then sum(PAID_NET_AMT) OVER (PARTITION BY SRC_MBR_ID,CLAIM_TYPE_DIM_ID order by SRC_MBR_ID) else 0 end RX_COST_OF_CARE1, case when CLAIM_TYPE_DIM_ID IN (1,2,3,6,7,8) then    sum(PAID_NET_AMT) OVER (PARTITION BY SRC_MBR_ID order by SRC_MBR_ID) end TOTAL_COST_OF_CARE     from  (select  src_mbr_id,  CLAIM_TYPE_DIM_ID,WHOLE_CLAIM_STATUS_DIM_ID,PRIMARY_SVC_DATE,claim_status_dim_id,                 POST_DATE,     paid_net_amt,claim_number, line_number,SUB_LINE_CODE  from compact.fact_claim clm inner join NORTHSTAR.ns_member ns on src_mbr_id=memb_dim_id and state_code='FL'  LEFT JOIN COMPACT.DIM_PLACE_OF_SVC PLSVC ON clm.PLACE_OF_SVC_1_DIM_ID= PLSVC.PLACE_OF_SVC_DIM_ID where   CLAIM_TYPE_DIM_ID IN (1,2,6,7,3,8) AND WHOLE_CLAIM_STATUS_DIM_ID=1    and claim_status_dim_id in (SELECT claim_status_dim_id FROM COMPACT.dim_claim_status WHERE claim_status_type_dim_id <> 3)         AND PRIMARY_SVC_DATE between    :1 and :2 and post_date between :1 and add_months(:2,3)   )))";
               var stmtAmtCalc = snowflake.createStatement({ sqlText: SQLcmdAmtCalc, binds: [START_DATE, END_DATE]} );
                var resultAmtCalc = stmtAmtCalc.execute();
                resultAmtCalc.next();
                var resultAmtCalc = resultAmtCalc.getColumnValue(1);

                SQLcmdAmtCalcupd="UPDATE NORTHSTAR.NS_MEMBER ns SET ns.BH_COST_OF_CARE=amt.BH_COST_OF_CARE ,ns.MED_COST_OF_CARE=amt.MED_COST_OF_CARE,ns.RX_COST_OF_CARE =amt.RX_COST_OF_CARE,ns.TOTAL_COST_OF_CARE=amt.TOTAL_COST_OF_CARE   from  NORTHSTAR.NS_AMT_FL amt where ns.src_mbr_id=amt.src_mbr_id and STATE_CODE='FL'";
                 var stmtAmtCalcupd = snowflake.createStatement({ sqlText: SQLcmdAmtCalcupd} );
                  var resultAmtCalcupd = stmtAmtCalcupd.execute();
                  resultAmtCalcupd.next();
                  var resultAmtCalcupd = resultAmtCalcupd.getColumnValue(1);

		 		   	   SQLcmdNSVISIT= "create or replace temporary table NORTHSTAR.NS_VISIT as (select distinct memb_dim_id,medicaid_no,sum(MEDICAL_VISITs) over (partition by src_mbr_id) med_visit,sum(BH_VISITs) over (partition by src_mbr_id) BH_VISIT from  (select src_mbr_id,ns.medicaid_no,memb_dim_id,add_months( :2  ,-6) beg_dt,claim_number,place_of_svc_code,CLM.PRIMARY_SVC_DATE ,admission_date,CALC_ADMITS,CLAIM_TYPE_DIM_ID ,case when CLM.PRIMARY_SVC_DATE between add_months( :2  ,-6) and  :2 and admission_date between  add_months( :2  ,-6)and  :2 and place_of_svc_code not in ('23')and CLAIM_TYPE_DIM_ID  in (1,2) then calc_admits else 0 end MEDICAL_VISITs,case when CLM.PRIMARY_SVC_DATE between add_months( :2  ,-6) and  :2 and admission_date between  add_months( :2  ,-6)and  :2 and CLAIM_TYPE_DIM_ID  in (6,7) then calc_admits else 0 end BH_VISITs from compact.fact_claim clm inner join NORTHSTAR.NS_MEMBER ns on src_mbr_id=memb_dim_id LEFT JOIN COMPACT.DIM_PLACE_OF_SVC PLSVC ON clm.PLACE_OF_SVC_1_DIM_ID= PLSVC.PLACE_OF_SVC_DIM_ID where  CLAIM_TYPE_DIM_ID IN (1,2,6,7,3,8) AND WHOLE_CLAIM_STATUS_DIM_ID=1  and claim_status_dim_id in (SELECT claim_status_dim_id FROM COMPACT.dim_claim_status WHERE claim_status_type_dim_id <> 3)  AND PRIMARY_SVC_DATE between  :1 and :2 and post_date between :1 and add_months(:2,3) and state_code='FL')) ";
		 			   var stmtNSVISIT = snowflake.createStatement({ sqlText: SQLcmdNSVISIT, binds: [START_DATE, END_DATE]} );

		                 var resultNSVISIT = stmtNSVISIT.execute();
		                 resultNSVISIT.next();
		                 var resultNSVISIT = resultNSVISIT.getColumnValue(1);

		   		   	   SQLcmdNSVISITupd= "update NORTHSTAR.NS_MEMBER ns set ns.MED_VISIT=vi.MED_VISIT,ns.BH_VISIT=vi.BH_VISIT from  NORTHSTAR.NS_VISIT vi where ns.src_mbr_id=vi.memb_dim_id AND STATE_CODE='FL' ";
		                  var stmtNSVISITupd = snowflake.createStatement({ sqlText: SQLcmdNSVISITupd} );
		                   var resultNSVISITupd = stmtNSVISITupd.execute();
		                   resultNSVISITupd.next();
		                   var resultNSVISITupd = resultNSVISITupd.getColumnValue(1);

		 					   SQLcmdNSVISIT1= "create or replace temporary table NORTHSTAR.NS_VISIT as (select distinct memb_dim_id,medicaid_no,sum(ER_VISITs) over (partition by src_mbr_id) ER_VISIT from  (select distinct  src_mbr_id,ns.medicaid_no,memb_dim_id,add_months( :2  ,-6) beg_dt,place_of_svc_code,CLM.PRIMARY_SVC_DATE CLAIM_TYPE_DIM_ID ,case when CLM.PRIMARY_SVC_DATE between add_months( :2  ,-6) and  :2 and place_of_svc_code in ('23') and CLAIM_TYPE_DIM_ID  in (1,2,6,7) then calc_visits else 0 end ER_VISITs from compact.fact_claim clm inner join NORTHSTAR.NS_MEMBER ns on src_mbr_id=memb_dim_id LEFT JOIN COMPACT.DIM_PLACE_OF_SVC PLSVC ON clm.PLACE_OF_SVC_1_DIM_ID= PLSVC.PLACE_OF_SVC_DIM_ID where  CLAIM_TYPE_DIM_ID IN (1,2,6,7,3,8) AND WHOLE_CLAIM_STATUS_DIM_ID=1  and claim_status_dim_id in (SELECT claim_status_dim_id FROM COMPACT.dim_claim_status WHERE claim_status_type_dim_id <> 3)  AND PRIMARY_SVC_DATE between  :1 and :2 and post_date between :1 and add_months(:2,3) and state_code='FL' ))";
		 		   var stmtNSVISIT1 = snowflake.createStatement({ sqlText: SQLcmdNSVISIT1, binds: [START_DATE, END_DATE]} );
		              var resultNSVISIT1 = stmtNSVISIT1.execute();
		              resultNSVISIT1.next();
		              var resultNSVISIT1 = resultNSVISIT1.getColumnValue(1);



		    	   SQLcmdNSVISITupd1= "update NORTHSTAR.NS_MEMBER ns set ns.ER_VISIT=vi.ER_VISIT from  NORTHSTAR.NS_VISIT vi where ns.src_mbr_id=vi.memb_dim_id AND STATE_CODE='FL' ";
		          var stmtNSVISITupd1 = snowflake.createStatement({ sqlText: SQLcmdNSVISITupd1} );
		           var resultNSVISITupd1 = stmtNSVISITupd1.execute();
		           resultNSVISITupd1.next();
		           var resultNSVISITupd1 = resultNSVISITupd1.getColumnValue(1);


	 				SQLcmdNSAcuity= "create or replace  temporary table NORTHSTAR.ACUITY as (select  case when tot_point=0 then 'low acuity'  when tot_point in (1,2) then 'medium acuity' when tot_point>=3 then 'high acuity' end Segment , * from (select sum (BH_POINT+MED_POINT+TOT_CC_POINT+MED_VISIT_POINT+ER_VISIT_POINT+BH_VISIT_POINT) over (partition by src_mbr_id) tot_point ,* from 	(select STATE_CODE,SRC_MBR_ID,BH_COST_OF_CARE,MED_COST_OF_CARE,RX_COST_OF_CARE,TOTAL_COST_OF_CARE,MED_VISIT,ER_VISIT,BH_VISIT,case when BH_COST_OF_CARE > (select amount from NORTHSTAR.CONFIG_ACUITY where state='FL' and TYPE='BH') then 1 else 0 end BH_Point,case when MED_COST_OF_CARE > (select amount from NORTHSTAR.CONFIG_ACUITY where state='FL' and TYPE='MED') then 1 else 0   end MED_Point,	case  when TOTAL_COST_OF_CARE > (select amount from NORTHSTAR.CONFIG_ACUITY where state='FL' and TYPE='TOT') then 1 else 0  end TOT_CC_Point,case  when MED_VISIT > 0  then 1 else 0  end MED_VISIT_Point,case  when ER_VISIT > 0 then 1  else 0  end ER_VISIT_Point,case  when BH_VISIT > 0 then 1 else 0 end BH_VISIT_Point from NORTHSTAR.NS_MEMBER where state_code='FL' order by BH_COST_OF_CARE  )))";
	 			   var stmtNSAcuity = snowflake.createStatement({ sqlText: SQLcmdNSAcuity});
	               var resultNSAcuity = stmtNSAcuity.execute();
	               resultNSAcuity.next();
	               var resultNSAcuity = resultNSAcuity.getColumnValue(1);

	 		   	   SQLcmdNSAcuityupd= "update NORTHSTAR.NS_MEMBER ns set ns.SEGMENT=vi.SEGMENT from  NORTHSTAR.ACUITY vi where ns.src_mbr_id=vi.src_mbr_id and ns.state_code=vi.state_code AND ns.STATE_CODE='FL' ";
	                var stmtNSAcuityupd = snowflake.createStatement({ sqlText: SQLcmdNSAcuityupd} );
	                 var resultNSAcuityupd = stmtNSAcuityupd.execute();
	                 resultNSAcuityupd.next();
	                 var resultNSAcuityupd = resultNSAcuityupd.getColumnValue(1);


     result11=resultCNupd
     return result11;


}
    catch (err) {
        result =  "Failed: Code: " + err.code + ", State: " + err.state;
        result += "\\n Message: " + err.message;
        result += "\\n Stack Trace:\\n" + err.stackTraceTxt;
		throw result;
       }
$$
;