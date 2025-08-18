CREATE OR REPLACE FUNCTION  COMPACT.rClaimsDLOCDerivInputs(PROC_CD VARCHAR, POS_CD VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
AS
$$
        var PROC_CD = PROC_CD;
        var POS_CD = POS_CD;
        if (PROC_CD == '90899') {
                    if (POS_CD == '01' ||   POS_CD == '23' || POS_CD == '81')
                        {
                       clm_lvl_of_care_deriv_txt = 'Ancillary';
                       clm_lvl_of_srvc_deriv_cd  = 'ANC';
                       clm_unit_typ_cd = 1;
                       return (clm_lvl_of_care_deriv_txt+'#'+ clm_lvl_of_srvc_deriv_cd+'#'+clm_unit_typ_cd+'1');

                        }

                       else if (POS_CD  == '03' || POS_CD  == '12'  || POS_CD == '31' || POS_CD == '32' ||  POS_CD == '33' || POS_CD == '34')
                             {
                             clm_lvl_of_care_deriv_txt = 'Outpatient';
                             clm_lvl_of_srvc_deriv_cd  = 'XOT';
                             clm_unit_typ_cd = ' ';
                             return (clm_lvl_of_care_deriv_txt+'#'+ clm_lvl_of_srvc_deriv_cd+'#'+clm_unit_typ_cd+'2');
                              }
                        else if (POS_CD  == '24')
                              {
                                  clm_lvl_of_care_deriv_txt = 'Professional Services';
                                  clm_lvl_of_srvc_deriv_cd  = ' ';
                                  clm_unit_typ_cd = ' ';
                                  return (clm_lvl_of_care_deriv_txt+'#'+ clm_lvl_of_srvc_deriv_cd+'#'+clm_unit_typ_cd+'3');
                              }
                      }

                     clm_lvl_of_care_deriv_txt = 'Structured Outpatient';
                     clm_lvl_of_srvc_deriv_cd  = 'XSO';
                     clm_unit_typ_cd =' ';
                     return (clm_lvl_of_care_deriv_txt+'#'+ clm_lvl_of_srvc_deriv_cd+'#'+clm_unit_typ_cd+'4');
$$
;
