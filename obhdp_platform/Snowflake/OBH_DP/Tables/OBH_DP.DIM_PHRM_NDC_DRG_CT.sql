CREATE TABLE IF NOT EXISTS OBH_DP.DIM_PHRM_NDC_DRG (
NDC_DRG_SYS_ID VARCHAR(50) NOT NULL,
PHRM_NDC_CD VARCHAR(12) NOT NULL CONSTRAINT UK_F_DIM_PHRM_NDC_DRG UNIQUE,
NDC_ROW_EFF_DT DATE NOT NULL,
NDC_ROW_END_DT DATE NULL,
AHFS_THRPTC_CLSS_CD VARCHAR(8) NULL,
NDC_BRND_NM VARCHAR(100) NULL,
NDC_DEA_DRG_ABUS_CD VARCHAR(1) NULL,
NDC_DSTRB_ID VARCHAR(6) NULL,
NDC_DSTRB_NM VARCHAR(100) NULL,
NDC_DOSE_FM_DESC VARCHAR(100) NULL,
NDC_ADMIN_RTE_TXT VARCHAR(100) NULL,
NDC_AVL_CD VARCHAR(1) NULL,
NDC_CATGY_CD VARCHAR(1) NULL,
NDC_FM_CD VARCHAR(2) NULL,
NDC_OBSLT_DT DATE NULL,
NDC_PRC_TYP_CD VARCHAR(1) NULL,
NDC_STRGTH_DESC VARCHAR(50) NULL,
NDC_STRGTH_NBR NUMBER(15,5) NULL,
NDC_STRGTH_UNIT_DESC VARCHAR(50) NULL,
NDC_STRGTH_VOL_NBR NUMBER(7,3) NULL,
NDC_STRGTH_VOL_UNIT_DESC VARCHAR(50) NULL,
NDC_XPND_DRG_DESC VARCHAR(20) NULL,
NDC_XPND_ORAG_BK_CD VARCHAR(2) NULL,
NDC_GNRC_IND_CD VARCHAR(1) NULL,
NDC_GNRC_NM VARCHAR(100) NULL,
NDC_GNRC_NBR VARCHAR(5) NULL,
NDC_GNRC_SEQ_NBR VARCHAR(14) NULL,
NDC_GDR_SPEC_DRG_CD VARCHAR(1) NULL,
NDC_GNRC_THRPTC_CLSS_CD VARCHAR(8) NULL,
NDC_HCFA_APRV_DT DATE NULL,
NDC_HIER_INGR_NBR VARCHAR(54) NULL,
NDC_HIER_INGR_SEQ_NBR VARCHAR(6) NULL,
NDC_INNOV_IND_CD VARCHAR(1) NULL,
NDC_LBL_CD VARCHAR(5) NULL,
NDC_PKG_CD VARCHAR(2) NULL,
NDC_PRDCT_CD VARCHAR(5) NULL,
NDC_PKG_DESC VARCHAR(10) NULL,
NDC_PKG_SZ_EQ_NBR NUMBER(11,3) NULL,
NDC_PKG_SZ_NBR NUMBER(11,3) NULL,
NDC_PREG_PCAUT_NBR VARCHAR(36) NULL,
NDC_REPKG_IND_CD VARCHAR(1) NULL,
NDC_SIDE_EFF_NBR VARCHAR(35) NULL,
NDC_SPEC_THRPTC_CLSS_CD VARCHAR(8) NULL,
NDC_STD_PKG_IND_CD VARCHAR(1) NULL,
NDC_STD_THRPTC_CLSS_CD VARCHAR(8) NULL,
NDC_UNIT_DOSE_IND_CD VARCHAR(1) NULL,
NDC_UNIT_OF_USE_IND_CD VARCHAR(1) NULL,
NDC_COMPRS_BRND_NM VARCHAR(100) NULL,
NDC_MEDCO_THRPTC_CLSS_CD VARCHAR(8) NULL,
EXT_AHFS_THRPTC_CLSS_CD VARCHAR(8) NULL,
SRC_REC_CD VARCHAR(4) NULL,
REC_CRT_DT DATE NULL,
REC_CRT_TM DATETIME NULL,
REC_UPD_DT DATE NULL,
REC_UPD_TM DATETIME NULL,
NDC_PROC_CD VARCHAR(10) NULL,
NDC_GPI_CD VARCHAR(14),
OBH_SCR_RL_LST VARIANT,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36),
CONSTRAINT PK_F_DIM_PHRM_NDC_DRG_SYS_ID PRIMARY KEY (NDC_DRG_SYS_ID)
);
COMMENT ON TABLE OBH_DP.DIM_PHRM_NDC_DRG IS 'This table contains NDC data for pharmacy claims.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PROC_CD IS 'NDC Procedure Code: describes the type of procedure performed or service provided. This procedure code is usually a CPT-4 OR HCPCS Code. Data Placement: left justified, space filled .   For valid values, see table PROCEDURE_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_DRG_SYS_ID IS 'National Drug System Identifier: Data warehouse system generated key';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.PHRM_NDC_CD IS 'National Drug Code (NDC): The unique code that identifies a drug product as defined by the National Drug Data File (all drug products regulated by the FDA must use an NDC).';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_ROW_EFF_DT IS 'NDC Drug Row Effective Date:  First date the row became effective in the source (Galaxy).';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_ROW_END_DT IS 'NDC Row End Date: Last date the row was effective in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.AHFS_THRPTC_CLSS_CD IS 'American Hospital Formulary Service Therapeutic Class Code: A code that identifies therapeutic category of drug according to the American Hospital Formulary Service (AHFS) classification system.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_BRND_NM IS 'NDC Brand Name: Provides the drug name on the package label and frequently is a trademark name. For non-branded generic products, the description will usually be the generic name.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_DEA_DRG_ABUS_CD IS 'NDC Enforcement Agency Drug Abuse Code: Indicates the abuse potential for a controlled drug as defined by the Drug Enforcement Agency (DEA) and is subject to change by federal regulation.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_DSTRB_ID IS 'NDC Distribution Identifier: The code identifying the drug distributor. It does not necessarily identify the actual drug manufacturer.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_DSTRB_NM IS 'NDC Distribution Name: The name of the drug distributor as listed on the drug label or indicated by the NDC. It does not necessarily identify the actual drug manufacturer.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_DOSE_FM_DESC IS 'NDC Dosage Form Description: Provides a description of how the drug is to be taken by the patient.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_ADMIN_RTE_TXT IS 'NDC Administration Route Textl Identifies the normal place or method by which a drug is administered to a patient.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_AVL_CD IS 'NDC Availability Code: Classifies a drug by its availability to the consumer according to federal specifications. For valid values, see table DRUG_AVAILABILITY_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_CATGY_CD IS 'NDC Category Code: Indicates that a drug product belongs to a category that is commonly treated as an exception in third party plans. For example antacids, smoking deterrents. For valid values, see table DRUG_CATEGORY_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_FM_CD IS 'NDC Form Code: Indicates the basic drug measurement unit for performing price calculations. For example, per tablet, per gram. For valid values, see table DRUG_FORM_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_OBSLT_DT IS 'NDC Obsolete Date: The Drug Obsolete Date is the date (or the estimated date) that the drug was declared obsolete (and replaced by another drug) or no longer available in the market place per the manufacturers notification.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PRC_TYP_CD IS 'NDC Pricing Type Code: Indicates if a drug product should be priced as a generic or brand drug. For valid values, see table DRUG_PRICING_TYPE_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STRGTH_DESC IS 'NDC Strength Description: description of drug potency in units of grams, milligrams, percentage, and other terms. This field includes needle sizes, length of devices, and release rates of transdermal patches.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STRGTH_NBR IS 'NDC Strength Number: describes the strength of a drug product. For example: given a strength of 250 MG, 250 is the strength number. Only single ingredient products have values in this field.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STRGTH_UNIT_DESC IS 'NDC Strength Unit Description: Provides a metric description of the strength of a drug product. For example, given a strength of 250 MG, MG is the strength unit.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STRGTH_VOL_NBR IS 'NDC Strength Volume Number: Indicates the volume or weight of the drug product that contains the indicated amounts of active ingredients. For example, given strength of 250 MG/5 ML, 5 is the strength volume.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STRGTH_VOL_UNIT_DESC IS 'NDC Strength Volume Unit Description: A metric measure indicating the volume or weight of the drug product containing the indicated amounts of active ingredients. For example, given strength of 250 MG/5ML, ML is the strength volume unit.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_XPND_DRG_DESC IS 'NDC Expand Drug Description';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_XPND_ORAG_BK_CD IS 'NDC Expand Orange Book Code';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GNRC_IND_CD IS 'NDC Generic Indicator Code: Indicates if the drug is multiple sourced. N=Single source. U=Unknown. Y=Multiple sources';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GNRC_NM IS 'NDC Generic Name: Contains the drug ingredient name for a specific drug as adopted by the United States Adopted Names. The chemical name is used when the USAN name is not available.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GNRC_NBR IS 'NDC Generic Number: A unique number representing the generic formulation of a drug and is specific to generic ingredient combination, route of administration and drug strength, across all drug forms.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GNRC_SEQ_NBR IS 'A unique number that represents a generic formulation of a drug product. Like the Generic Code Number, the Generic Code Sequence number is specific to generic ingredient combinations, route of administration, dosage form and drug strength. The generic code number may be the same for different dosage forms, but the sequence number is specific to a dosage form';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GDR_SPEC_DRG_CD IS 'NDC Gender Specific Drug Code: Identifies if the drug is used for a specific gender. Will identify as gender exclusive, gender likely, or gender neutral.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GNRC_THRPTC_CLSS_CD IS 'Indicates the classification of drugs according to the most common intended use as the broadest therapeutic groupings available. For example antihistamines, cardiac drugs, cough and cold preparations.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_HCFA_APRV_DT IS 'NDC Healthcare Finance Administration Approval Date: The date that the Federal Drug Administration approved the drug for use as supplied on the Healthcare Finance Administration (HCFA) quarterly tape.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_HIER_INGR_NBR IS 'NDC Hierarchical Ingredient Number: Includes up to nine hierarchical ingredient codes, each six characters in length. Each hierarchical ingredient code may have spaces in the fifth and sixth position.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_HIER_INGR_SEQ_NBR IS 'Provides a link between the Hierarchical Ingredient Number and a National Drug Code or a Generic Sequence Number. Can be used to identify a unique combination of ingredients for a drug independent of National Drug Code';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_INNOV_IND_CD IS 'Indicates if this product held the original patent for the drug. Since this may be assigned to multiple package sizes and to replacement National Drug Codes, more than one product may appear to be the innovator';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_LBL_CD IS 'NDC Label Code: Part of the National Drug Code (NDC), it is assigned by the Federal Drug Administration to identify manufacturers. The combination of NDC Label Code, NDC Package Code and NDC Product Code makes up a complete National Drug Code.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PKG_CD IS 'NDC Package Code: Identifies the container in which the drug product is sold. The combination of NDC Label Code, NDC Package Code and NDC Product Code makes up a complete National Drug Code.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PRDCT_CD IS 'A number assigned by the Federal Drug Association to identify a drug within a manufacturer. The combination of NDC Label Code, NDC Package Code and NDC Product Code makes up a complete National Drug Code.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PKG_DESC IS 'NDC Package Description: Describes the container in which the drug is sold.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PKG_SZ_EQ_NBR IS 'NDC Package Size Equivalent Number: Provides a conversion of Package Size Number for non-injectable products to non-metric or most commonly used package size descriptions. For example, 15 ml = 1/2 fluid ounce.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PKG_SZ_NBR IS 'Provides a conversion of Package Size Number for non-injectable products to non-metric or most commonly used package size descriptions. For example, 15 ml = 1/2 fluid ounce';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_PREG_PCAUT_NBR IS 'NDC Pregnancy Precaution Number: Indicates if this is the first, second, or subsequent refill for the prescription.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_REPKG_IND_CD IS 'NDC Repackaged Indicator Code: Indicates if a drug was repackage by a labeler. Labelers usually repackage drugs into unit of use or dispensable package sizes. N=No U=Unknown Y=Yes';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_SIDE_EFF_NBR IS 'NDC Side Effect Number: The number identifying the side affects that may be associated with a drug.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_SPEC_THRPTC_CLSS_CD IS 'Used to classify drugs according to the most common intended use. For example, medical supplies, muscle relaxants, Antiparkinson.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STD_PKG_IND_CD IS 'NDC Standard Package Indicator Code: Indicates if the drug is dispensed in the standard package size. N=No U=Unknown Y=Yes';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_STD_THRPTC_CLSS_CD IS 'NDC Standard Therapeutic Class Code: Used to classify drugs according to the most common intended use. For example, medical supplies, muscle relaxants, Antiparkinson. Data Placement: right justified prefixed with zero. For valid values, see table STANDARD_THERAPEUTIC_CLASS_CODE in Galaxy.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_UNIT_DOSE_IND_CD IS 'NDC Unit Dose Indicator: Indicates if the manufacturer packaged the drug at the unit dose level. N=No U=Unknown Y=Yes';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_UNIT_OF_USE_IND_CD IS 'NDC Unit of Use Indicator Code: Indicates if the package is supplied with appropriate labeling and (usually) child resistant closures and is appropriate to dispense as a unit. N=No U=Unkown Y=Yes';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_COMPRS_BRND_NM IS 'NDC Compressed Brand Name: The brand name with all non-alphabetic and non-numeric characters and spaces removed.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_MEDCO_THRPTC_CLSS_CD IS 'A code that identifies the therapeutic category of a drug according to Medcos proprietary classification system';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.EXT_AHFS_THRPTC_CLSS_CD IS 'Medi-Span is licensed by the American Society of Health-Care Pharmacists to use the American Hospital Formulary Service Classification Compilation Code (AHFS). The Extended AHFS Therapeutic Class Code is an eight-digit version of the six-digit AHFS Therapeutic Class Code in the A1 Record. If you receive the following warning message when joining this code table to any Fact tables please disregard: WARNING: Multiple lengths were specified for the BY variable (field name) by input data sets. This may cause unexpected results';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.SRC_REC_CD IS 'The source code representing the initial input system for a given record.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.REC_CRT_DT IS 'The create date of a record into the table.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.REC_CRT_TM IS 'The create time of a record into the table.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.REC_UPD_DT IS 'The update date of a record into the table.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.REC_UPD_TM IS 'The update time of a record into the table.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.NDC_GPI_CD IS 'Generic Product Identifier code used to see if two drugs are equivalent.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.OBH_SCR_RL_LST IS 'This field will list all the roles which have access to the data in the table.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.SF_INSERT_DATETIME IS 'Date the record was added to the OBHDP.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.SF_UPDATE_DATETIME IS 'Date the record was updated in the OBHDP.';
COMMENT ON COLUMN OBH_DP.DIM_PHRM_NDC_DRG.SF_PROCESS_ID IS 'Unique identifier assigned to the process that inserts/updates the data in OBHDP.';
