 CREATE TABLE IF NOT EXISTS ETL.SMART_ACO_PRODUCTS_L (
	ACCOUNT_ID VARCHAR(30),
	GROUP_NAME VARCHAR(100),
	REGION VARCHAR(12),
	STATE VARCHAR(2),
	LINE_OF_BUSINESS VARCHAR(6),
	LINE_OF_BUSINESS_DESC VARCHAR(40),
	PROGRAM_TYPE_CODE VARCHAR(12),
	PROGRAM_TYPE_DESC VARCHAR(40),
	PRODUCT_ROLLUP VARCHAR(255),
	PRODUCT_DETAIL VARCHAR(255),
	STATUS VARCHAR(20),
	IMPACT_ACCOUNT_ID VARCHAR(25),
	ADDITIONAL_COMMENTS VARCHAR(4000),
	INSERTED_DATETIME VARCHAR(35),
	UPDATED_DATETIME VARCHAR(35),
	PLAN_CODE VARCHAR(20),
	PLAN_DESC VARCHAR(240)
);

COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.ACCOUNT_ID IS '"ID" followed by 9 digit code. Sourced from Impact "Title" field, Unique identifier for a specific contract.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.GROUP_NAME IS 'ACO or Group name.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.REGION IS 'Region associated with the state group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.STATE IS '2-digit abbreviation for State where the group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.LINE_OF_BUSINESS IS 'The 3 digit LOB code - matches METR and Netops Datasets.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.LINE_OF_BUSINESS_DESC IS 'The description associated with the 3 digit code - matches METR and Netops datasets.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PROGRAM_TYPE_CODE IS 'Code associated with the more granular program/product info - matches the Netops dataset.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PROGRAM_TYPE_DESC IS 'Description associated with the more granular program/product info - matches the METR dataset.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PRODUCT_ROLLUP IS 'High-level grouping of the line item - used as a consistent identifier across markets.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PRODUCT_DETAIL IS 'Detailed l grouping of the line item - used as a consistent identifier across markets.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.STATUS IS 'Signifies whether the LOB/Program/Plan combo is included in the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.IMPACT_ACCOUNT_ID IS 'Links a group''s contracts across multiple contracts - sourced from the Impact field "Account_ID".';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.ADDITIONAL_COMMENTS IS 'Any notes/comments/unique terms of contract (4000 characters max for cell). Leave blank if does not apply.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PLAN_CODE IS 'Code associated with the most granular plan info - matches METR and Netops datasets.';
COMMENT ON COLUMN ETL.SMART_ACO_PRODUCTS_L.PLAN_DESC IS 'Description associated with the most granular plan info - matches METR and Netops datasets.';