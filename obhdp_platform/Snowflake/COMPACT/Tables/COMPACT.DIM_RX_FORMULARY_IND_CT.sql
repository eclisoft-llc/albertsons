CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_FORMULARY_IND
    (
        RX_FORMULARY_IND_DIM_ID INTEGER NOT NULL,
        RX_FORMULARY_IND VARCHAR(1) NOT NULL,
        RX_FORMULARY_IND_DESC VARCHAR(25),  
        DW_INSERT_DATETIME DATETIME ,
        DW_UPDATE_DATETIME DATETIME ,
        SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
        SF_PROCESS_ID VARCHAR(36),
        CONSTRAINT PK_F_DIM_RX_FRMLRY_IND PRIMARY KEY (RX_FORMULARY_IND_DIM_ID)
    );
COMMENT ON TABLE COMPACT.DIM_RX_FORMULARY_IND IS 'This table stores attributes that describe if the Pharmacy claim prescription is on a list or formulary. ';
COMMENT ON COLUMN COMPACT.DIM_RX_FORMULARY_IND.RX_FORMULARY_IND_DIM_ID IS 'Represents the link to DM_RX_FORMULARY_IND table that indicates if the Pharmacy claim prescription is on a list or formulary. Values include 0 = ''U'' UNKNOWN, 1 = ''Y'' Formulary, 2 = ''N'' Non-Formulary or -1 = - N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_FORMULARY_IND.RX_FORMULARY_IND IS 'Indicates if the Pharmacy claim prescription is on a list or formulary. Values include ''U'' = UNKNOWN, ''Y'' = Formulary, ''N'' = Non-Formulary or - N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_FORMULARY_IND.RX_FORMULARY_IND_DESC IS 'Describes if the Pharmacy claim prescription is on a list or formulary. Values include UNKNOWN, Formulary, Non-Formulary or N/A.';
COMMENT ON COLUMN COMPACT.DIM_RX_FORMULARY_IND.DW_INSERT_DATETIME IS 'Date the record was added to the Data Warehouse.';
COMMENT ON COLUMN COMPACT.DIM_RX_FORMULARY_IND.DW_UPDATE_DATETIME IS 'Date the record was updated in the Data Warehouse.';
