CREATE OR REPLACE FUNCTION COMPACT.RCLAIMSDLOC(
            "PROC_TYP_CD" VARCHAR(16777216),
            "MD_DERIV_PROC_RVNU_CD" VARCHAR(16777216),
            "SRVC_UNIT_CNT" VARCHAR(16777216),
            "MH_SA_CD" VARCHAR(16777216),
            "AUTH_NBR" VARCHAR(16777216),
            "UNIT_TYP_CD" VARCHAR(16777216),
            "LKP_DRV_FOUND" VARCHAR(16777216),
            "LKP_DRV_LVL_OF_CARE_TXT" VARCHAR(16777216),
            "LKP_DRV_LVL_OF_SRVC" VARCHAR(16777216),
            "LKP_AUTH_FOUND" VARCHAR(16777216),
            "LKP_AUTH_LVL_OF_CARE_TXT" VARCHAR(16777216),
            "LKP_AUTH_LVL_OF_SRVC" VARCHAR(16777216),
            "LKP_CLM_CATGY" VARCHAR(16777216),
            "LKP_AUTH_CATGY" VARCHAR(16777216),
            "BOTH_INTERMEDIATE" VARCHAR(16777216))

RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS
$$
          var          PROC_TYP_CD  = PROC_TYP_CD ;
          var          PROC_CD  = MD_DERIV_PROC_RVNU_CD ;
          var          SRVC_UNIT_CNT         = SRVC_UNIT_CNT      ;
          var          MH_SA_CD     =         MH_SA_CD      ;
          var          AUTH_NBR      =         AUTH_NBR       ;
          var          UNIT_TYP_CD =         UNIT_TYP_CD  ;
          var          LKP_DRV_FOUND       =         LKP_DRV_FOUND        ;
          var          LKP_DRV_LVL_OF_CARE_TXT =         LKP_DRV_LVL_OF_CARE_TXT  ;
          var          LKP_DRV_LVL_OF_SRVC         =         LKP_DRV_LVL_OF_SRVC         ;
          var          LKP_AUTH_FOUND     =         LKP_AUTH_FOUND      ;
          var          LKP_AUTH_LVL_OF_CARE_TXT           =         LKP_AUTH_LVL_OF_CARE_TXT            ;
          var          LKP_AUTH_LVL_OF_SRVC      =         LKP_AUTH_LVL_OF_SRVC       ;
          var          LKP_CLM_CATGY       =         LKP_CLM_CATGY        ;
          var          LKP_AUTH_CATGY      =         LKP_AUTH_CATGY      ;
          var    BOTH_INTERMEDIATE        =         BOTH_INTERMEDIATE  ;
          var    CLM_SRVC_UNIT_DERIV_CNT = 999;

          var reg = /^\d+$/;



          if ( PROC_CD == '90899' && SRVC_UNIT_CNT > 30 && LKP_DRV_FOUND != 0 && UNIT_TYP_CD != 0 )
          {
                 LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                 LVL_OF_SRVC_DERIV= 'TRA';
                 CLM_SRVC_UNIT_DERIV_CNT= '1';
                return (LVL_OF_CARE_DERIV_TXT +'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#1');
          }


                else if (PROC_CD == '90899' && SRVC_UNIT_CNT > 30 && LKP_DRV_FOUND == 0 && UNIT_TYP_CD == 0)
                     {
                     LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                     LVL_OF_SRVC_DERIV= 'ANC';
                     CLM_SRVC_UNIT_DERIV_CNT = '1';
                     return (LVL_OF_CARE_DERIV_TXT +'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#2');
                    }



            if (PROC_CD == '90899' && SRVC_UNIT_CNT > 30 && LKP_DRV_FOUND == 'Y' && LKP_AUTH_FOUND != 'Y' &&  BOTH_INTERMEDIATE != 'Y')
                {
                 if (UNIT_TYP_CD != null && UNIT_TYP_CD.LENGTH  > 0)
                    {
                      LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                      LVL_OF_SRVC_DERIV= 'ANC';
                      CLM_SRVC_UNIT_DERIV_CNT = 1;
                      return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#3a');
                    }
                    else
                    {
                        LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                        LVL_OF_SRVC_DERIV= 'ANC';
                        CLM_SRVC_UNIT_DERIV_CNT = SRVC_UNIT_CNT;
                        return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#3b');
                    }
                  }
              else
                    {
                        LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                        LVL_OF_SRVC_DERIV= 'ANC';
                        CLM_SRVC_UNIT_DERIV_CNT = 1;
                        return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#3c');
                    }


             if (PROC_CD == '90899' && SRVC_UNIT_CNT > 30 && LKP_AUTH_FOUND == 'Y' &&  BOTH_INTERMEDIATE == 'Y')
                     {

                      if (LKP_AUTH_FOUND  == 'Y')
                          {
                            if (LKP_AUTH_LVL_OF_CARE_TXT == 'Partial Hospitalization')
                                {
                                LVL_OF_CARE_DERIV_TXT =  'Day Treatment';
                                LVL_OF_SRVC_DERIV= LKP_AUTH_LVL_OF_SRVC;
                                CLM_SRVC_UNIT_DERIV_CNT = 1;
                                return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#5a');
                                }
                            else
                                {
                                 LVL_OF_CARE_DERIV_TXT = LKP_AUTH_LVL_OF_CARE_TXT;
                                 LVL_OF_SRVC_DERIV=  LKP_AUTH_LVL_OF_SRVC;
                                 return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#5b');
                                 }
                            }

                            if ( LVL_OF_CARE_DERIV_TXT != null && LVL_OF_CARE_DERIV_TXT.length > 0  && LKP_DRV_LVL_OF_CARE_TXT != null &&  LKP_DRV_LVL_OF_CARE_TXT.length   > 0)
                                    {

                                    if (LKP_AUTH_LVL_OF_CARE_TXT  == 0  &&   LVL_OF_SRVC_DERIV== 0)
                                      {
                                      LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                                      LVL_OF_SRVC_DERIV=     'ANC';
                                      CLM_SRVC_UNIT_DERIV_CNT = 1;
                                      return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#5c');
                                      }
                            }


                 if (PROC_CD == '90899' && SRVC_UNIT_CNT > 30 && LKP_AUTH_FOUND ==  'Y')
                     {
                        if (LKP_AUTH_LVL_OF_CARE_TXT != 'Partial Hospitalization'  ||
                              LKP_AUTH_LVL_OF_CARE_TXT != 'Structured Outpatient'    ||
                              LKP_AUTH_LVL_OF_CARE_TXT != 'Residential'             ||
                              LKP_AUTH_LVL_OF_CARE_TXT != 'Recovery Home')
                              {



                              if (LKP_AUTH_FOUND !=  'Y'    &&
                                     LKP_CLM_CATGY != null && LKP_CLM_CATGY.length > 0    &&
                                     LKP_AUTH_CATGY != null && LKP_AUTH_CATGY.length  > 0  &&
                                     reg.test(LKP_CLM_CATGY)  && reg.test(LKP_AUTH_CATGY.ascii) )
                                    {
                                          LVL_OF_CARE_DERIV_TXT = LKP_DRV_LVL_OF_CARE_TXT;
                                          LVL_OF_SRVC_DERIV= LKP_DRV_LVL_OF_SRVC;
                                          return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#6a');
                                    }

                                  }

                        else
                             {

                      if (LKP_AUTH_FOUND  != 'Y')
                          {
                            if (LKP_AUTH_LVL_OF_CARE_TXT != 'Partial Hospitalization')
                                {
                                LVL_OF_CARE_DERIV_TXT =  'Day Treatment';
                                LVL_OF_SRVC_DERIV= LKP_AUTH_LVL_OF_SRVC;
                                CLM_SRVC_UNIT_DERIV_CNT = 1;
                                return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#6b');
                                }
                            else
                                {
                                 LVL_OF_CARE_DERIV_TXT = LKP_AUTH_LVL_OF_CARE_TXT;
                                 LVL_OF_SRVC_DERIV=  LKP_AUTH_LVL_OF_SRVC;
                                 CLM_SRVC_UNIT_DERIV_CNT = 1;
                                 return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#6c');
                                 }
                            }

                            if ( LVL_OF_CARE_DERIV_TXT != null && LVL_OF_CARE_DERIV_TXT.length > 0  &&  LKP_DRV_LVL_OF_CARE_TXT != null && LKP_DRV_LVL_OF_CARE_TXT.length   > 0)
                                    {
                                            if (LKP_AUTH_LVL_OF_CARE_TXT  == 0  &&   LVL_OF_SRVC_DERIV== 0)
                                          {
                                          LVL_OF_CARE_DERIV_TXT = 'Ancillary';
                                          LVL_OF_SRVC_DERIV=     'ANC';
                                          CLM_SRVC_UNIT_DERIV_CNT = 1;
                                          return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+LVL_OF_SRVC_DERIV+'#6d');
                                          }
                                    }
                                }
                            }

                                    else
                                    {
                                          LVL_OF_CARE_DERIV_TXT = LKP_DRV_LVL_OF_CARE_TXT;
                                          LVL_OF_SRVC_DERIV= LKP_DRV_LVL_OF_SRVC;
                                          CLM_SRVC_UNIT_DERIV_CNT = 1;
                                          return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#6e');

                                    }
                             }
                          else
                               {
                                          LVL_OF_CARE_DERIV_TXT = LKP_DRV_LVL_OF_CARE_TXT;
                                          LVL_OF_SRVC_DERIV= LKP_DRV_LVL_OF_SRVC;
                                          CLM_SRVC_UNIT_DERIV_CNT = 1;
                                          return (LVL_OF_CARE_DERIV_TXT+'#'+ LVL_OF_SRVC_DERIV+'#'+CLM_SRVC_UNIT_DERIV_CNT+'#6f');
                                }

$$
;
