CREATE TABLE IF NOT EXISTS  ETL.SMART_ACO_TIN_LIST_L(
	ACCOUNT_ID VARCHAR(30),
	GROUP_NAME VARCHAR(100),
	REGION VARCHAR(12),
	STATE VARCHAR(2),
	TIN VARCHAR(12),
	VENDOR_NAME VARCHAR(255),
	TIN_EFF_DATE VARCHAR(35),
	TIN_END_DATE VARCHAR(35),
	TIN_EFF_DATE_CIP VARCHAR(35),
	TIN_END_DATE_CIP VARCHAR(35),
	TIN_STATUS VARCHAR(12),
	IMPACT_ACCOUNT_ID VARCHAR(25),
	ADDITIONAL_COMMENTS VARCHAR(4000),
	INSERTED_DATETIME VARCHAR(35),
	UPDATED_DATETIME VARCHAR(35),
	QFR_TIN_FLAG VARCHAR(10)
);

COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.ACCOUNT_ID IS '"ID" followed by 9 digit code. Sourced from Impact "Title" field, Unique identifier for a specific contract.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.GROUP_NAME IS 'ACO or Group name.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.REGION IS 'Region associated with the state group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.STATE IS '2-digit abbreviation for State where the group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN IS '9 digit numerical ID of the provider(s) included in the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.VENDOR_NAME IS 'Vendor_Name from data tables/name of PCP group that is included in the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN_EFF_DATE IS 'Date TIN is effective in the contract (typically aligns with YR_1_start_date/effective_date).';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN_END_DATE IS 'Date TIN terms in the contract (typically aligns with contract_end_date).';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN_EFF_DATE_CIP IS 'Date TIN is effective for the CIP portion of the contract, if applicable.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN_END_DATE_CIP IS 'Date TIN terms for the CIP portion of the contract, if applicable.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.TIN_STATUS IS 'Automated Field: Status of TIN (active - currently included in measurement period of contract, inactive - not currently part of the contracts active year). Contract Status should be used to determine if scorecards are still outstanding.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.IMPACT_ACCOUNT_ID IS 'Links a group''s contracts across multiple contracts - sourced from the Impact field "Account_ID".';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.ADDITIONAL_COMMENTS IS 'Any notes/comments/unique terms of contract (4000 characters max for cell). Leave blank if does not apply.';
COMMENT ON COLUMN ETL.SMART_ACO_TIN_LIST_L.QFR_TIN_FLAG IS 'Automated Field: Status of whether this TIN is included in QFR reporting as of the current point in time.';