CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_PLAN_DRUG_STATUS (
    PLAN_DRUG_STATUS_DIM_ID NUMBER NOT NULL,
    PLAN_DRUG_STATUS VARCHAR(3) NOT NULL,
    PLAN_DRUG_STATUS_DESC VARCHAR(50),
    DW_INSERT_DATETIME DATE NOT NULL,
    DW_UPDATE_DATETIME DATE NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PK_PLAN_DRUG_STATUS_DIM_ID PRIMARY KEY (PLAN_DRUG_STATUS_DIM_ID),
    CONSTRAINT UK_PLAN_DRUG_STATUS UNIQUE (PLAN_DRUG_STATUS)
);
COMMENT ON TABLE COMPACT.DIM_RX_PLAN_DRUG_STATUS IS 'This table stores attributes that describe the plan or program prescription status. The prescription drug might be on the list or formulary. ';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_DRUG_STATUS.PLAN_DRUG_STATUS_DIM_ID IS 'Represents the link to the DIM_RX_PLAN_DRUG_STATUS table describing the plan or program prescription status. The prescription drug might be on the list or formulary. Values are 1 = C non-formulary, 2 = F Formulary, 3 = G non-formulary, 4 = K formulary, non-max benefit, 5 = L non-formulary, non-max benefit, 6 = S S, 7 = 5 5, 0 = UNK UNKNOWN and -1 = N/A N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_DRUG_STATUS.PLAN_DRUG_STATUS IS 'Code that represents the plan or program prescription status. The prescription drug might be on the list or formulary. Values are C = non-formulary, F = Formulary, G = non-formulary, K = formulary, non-max benefit, L = non-formulary, non-max benefit, S = S, 5 = 5, UNK = UNKNOWN and N/A = N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_DRUG_STATUS.PLAN_DRUG_STATUS_DESC IS 'Describes the plan or program prescription status. The prescription drug might be on the list or formulary. Values are non-formulary, Formulary, non-formulary, formulary, non-max benefit, non-formulary, non-max benefit, S, 5, UNKNOWN and N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_DRUG_STATUS.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_PLAN_DRUG_STATUS.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
