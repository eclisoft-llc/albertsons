CREATE TABLE IF NOT EXISTS COMPACT.DIM_RX_PRIORAUTH_QUALIFIER (
    RX_PRIORAUTH_QUALIFIER_DIM_ID NUMBER NOT NULL,
    RX_PRIOR_AUTH_QUALIFIER VARCHAR(3) NOT NULL,
    RX_PRIOR_AUTH_QUALIFIER_DESC VARCHAR(40),
    DW_INSERT_DATETIME DATETIME NOT NULL,
    DW_UPDATE_DATETIME DATETIME NOT NULL,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
    CONSTRAINT PKRX_PRIORAUTH_QUALIFIER_DIM_ID PRIMARY KEY (RX_PRIORAUTH_QUALIFIER_DIM_ID),
    CONSTRAINT UK_RX_PRIOR_AUTH_QUALIFIER UNIQUE (RX_PRIOR_AUTH_QUALIFIER)
);
COMMENT ON TABLE COMPACT.DIM_RX_PRIORAUTH_QUALIFIER IS 'The table is a one time load and it represents prior authorization/medical certification qualifier';
COMMENT ON COLUMN COMPACT.DIM_RX_PRIORAUTH_QUALIFIER.RX_PRIORAUTH_QUALIFIER_DIM_ID IS 'This column is auto generated and it gets incremented for every value being added and mainly used in fact tables';
COMMENT ON COLUMN COMPACT.DIM_RX_PRIORAUTH_QUALIFIER.RX_PRIOR_AUTH_QUALIFIER IS 'This column represents whether a member is qualified for medical certification or is he authorized or not below values represents the same as blank = not used,0= not specified,1= prior authorization,2= medical certification,3= epsdt,4= exemption from co-pay,5= exemption from rx limits,6= family planning indicator,7= temporary assistance to ne';
COMMENT ON COLUMN COMPACT.DIM_RX_PRIORAUTH_QUALIFIER.RX_PRIOR_AUTH_QUALIFIER_DESC IS 'This column represents the description for RX_PRIOR_AUTH_QUALIFIER field for ex:1= prior authorization';
COMMENT ON COLUMN COMPACT.DIM_RX_PRIORAUTH_QUALIFIER.DW_INSERT_DATETIME IS 'This table represents the date value as when the record got updated';
