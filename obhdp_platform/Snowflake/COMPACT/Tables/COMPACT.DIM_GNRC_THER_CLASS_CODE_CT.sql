CREATE TABLE IF NOT EXISTS COMPACT.DIM_GNRC_THER_CLASS_CODE (
GNRC_THER_CLASS_DIM_ID NUMBER(9) NOT NULL CONSTRAINT PK_F_DIM_GNRC_THER_CLASS_CODE PRIMARY KEY,
GNRC_THER_CLASS_CODE VARCHAR2(2) CONSTRAINT UK_F_DIM_GNRC_THER_CLASS_CODE UNIQUE,
GNRC_THER_CLASS_DESC VARCHAR2(45),
DW_INSERT_DATETIME DATETIME,
DW_UPDATE_DATETIME DATETIME,
SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
SF_PROCESS_ID VARCHAR(36)
);
COMMENT ON TABLE COMPACT.DIM_GNRC_THER_CLASS_CODE IS 'This table stores attributes that describe National Drug codes found on the DIM_NDC_CODE table into one of five company therapeutic classifications. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.GNRC_THER_CLASS_DIM_ID IS 'Represents the link to DIM_AHFS_THER_CLASS_CODE table further defining the National Drug codes found on the DIM_NDC_CODE table into one of five company therapeutic classifications. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.GNRC_THER_CLASS_CODE IS 'Code for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.GNRC_THER_CLASS_DESC IS 'Description for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.SF_INSERT_DATETIME IS 'Date the record was added to the OBHDP.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.SF_UPDATE_DATETIME IS 'Date the record was updated in the OBHDP.';
COMMENT ON COLUMN COMPACT.DIM_GNRC_THER_CLASS_CODE.SF_PROCESS_ID IS 'Unique identifier assigned to the process that inserts/updates the data in OBHDP.';
