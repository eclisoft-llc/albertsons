CREATE TABLE IF NOT EXISTS FOUNDATION.MBR_COV (
MBR_COV_SYS_ID VARCHAR(100) NOT NULL CONSTRAINT PK_F_MBR_COV_SYS_ID PRIMARY KEY COMMENT 'Unique identifier for the record.',
SRC_REC_CD VARCHAR(5) NOT NULL COMMENT 'The source code representing the initial input system for a given record.',
SBSCR_ID VARCHAR(20) NOT NULL COMMENT 'The source system subscriber number given to a member. ',
SSN VARCHAR(9) NULL COMMENT 'Social Security Number',
MDCR_NBR varchar(12) NULL COMMENT 'Unique identfieir assigned to an individual enrolled in Medicare and Retirement',
MDCD_NBR VARCHAR(20) NULL COMMENT 'Unique identifier assigned to an individual enrolled in Medicaid program',
DCSD_DT DATE NULL COMMENT 'Represents the Date of death of the Member',
SRC_INDV_ID VARCHAR(12) COMMENT 'Represents the unique identifier assigned by the soure system to an individal irrespective of LOB',
IMDM_ID VARCHAR(25) COMMENT 'Represents the unique identifier [eid] assigned by the enterprise individual identity mamagement to an individal'
EDWPERSID VARCHAR(25) COMMENT 'Represents the unique identifier assigned by the Behavioral Health to an individual',
COV_TYP_CD varchar(1) COMMENT 'Represents the type of coverage the member is enrolled in medical, behavioral, rx',
COV_TYP_DESC VARCHAR(25) COMMENT 'Describes the type of coverage the member is enrolled in medical, behavioral, rx',
COV_STRT_DT DATE COMMENT 'Represents the date at which the members health insurance plan is effective. Individual members can have several segments of effective and term dates.', 
COV_END_DT DATE COMMENT 'Represents the date at which the member is terminated from the members health insurance plan.  Individual members can have several segments of effective and term dates.',
UHG_SEG_NM VARCHAR(50) comment 'Describes the united health group line of business name [Community & State, Employer & Individual, Medicare & Retirement, OptumRx, OptumHealth]',
ACCT_NM VARCHAR(50) comment 'Describes the name under which the account holds the eligibility with UHG',
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
);

