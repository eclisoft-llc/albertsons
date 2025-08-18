CREATE TABLE IF NOT EXISTS COMPACT.DIM_DATE_YEAR (
	YEAR_DIM_ID INTEGER NOT NULL,
	YEAR_NBR INTEGER,
	DW_INSERT_DATETIME DATETIME,
	DW_UPDATE_DATETIME DATETIME,
	YEAR_BEGIN_DATE DATE,
	YEAR_END_DATE DATE,
	CURR_YEAR_IND INTEGER,
    SF_INSERT_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_UPDATE_DATETIME DATETIME DEFAULT CURRENT_TIMESTAMP,
    SF_PROCESS_ID VARCHAR(36),
	CONSTRAINT PK_DIM_DATE_YEAR PRIMARY KEY (YEAR_DIM_ID)
);
COMMENT ON TABLE COMPACT.DIM_DATE_YEAR IS 'This table stores attributes that describe multiple ways for selecting year dates for reporting.';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.YEAR_DIM_ID IS 'This column is auto generated and it gets incremented for every value being added and mainly used in fact tables';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.YEAR_NBR IS 'This column store the 4 digit year.';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.DW_INSERT_DATETIME IS 'This table represents the date value as when the record got inserted';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.DW_UPDATE_DATETIME IS 'This table represents the date value as when the record got updated';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.YEAR_BEGIN_DATE IS 'This column store the beginning date of the year.';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.YEAR_END_DATE IS 'This column store the ending date of the year.';
COMMENT ON COLUMN COMPACT.DIM_DATE_YEAR.CURR_YEAR_IND IS 'This column store the number of years from CURRENT DATE to the beginning year.';