CREATE TABLE IF NOT EXISTS ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L (
SPEC_THER_CLASS_DIM_ID VARCHAR(50) NOT NULL CONSTRAINT PK_DIM_SPEC_THER_CLASS_CODE PRIMARY KEY,
SPEC_THER_CLASS_CODE VARCHAR(3) CONSTRAINT UK_DIM_SPEC_THER_CLASS_CODE UNIQUE,
SPEC_THER_CLASS_DESC VARCHAR(100),
DW_INSERT_DATETIME VARCHAR(35),
DW_UPDATE_DATETIME VARCHAR(35)
);
COMMENT ON TABLE ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L IS 'This table stores attributes that describe the National Drug codes found on the DIM_NDC_CODE table into one of five company therapeutic classifications. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L.SPEC_THER_CLASS_DIM_ID IS 'Represents the link to DIM_AHFS_THER_CLASS_CODE table further defining the National Drug codes found on the DIM_NDC_CODE table into one of five company therapeutic classifications. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L.SPEC_THER_CLASS_CODE IS 'Code for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L.SPEC_THER_CLASS_DESC IS 'Description for therapeutic classification. i.e. Brand name in NDC table = Tylenol Cold AND Flu Severe, SPEC description is NON - Narc Antitus - 1st Gen anthist-Decon-Analges, AHFS description is Antitussives, GNRC description is Cough/Cold preparations, Medco description is Decongestant/Antihistamines and STD description is Cold and Cough preparations.';
COMMENT ON COLUMN ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN ETL.SMART_DIM_SPEC_THER_CLASS_CODE_L.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
