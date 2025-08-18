CREATE TABLE IF NOT EXISTS ETL.SMART_ACO_CONTRACTS_L (
	ACCOUNT_ID VARCHAR(30),
	GROUP_NAME VARCHAR(100),
	REGION VARCHAR(12),
	STATE VARCHAR(2),
	COMPANY_DIM_ID NUMBER,
	PROGRAM_DESC VARCHAR(30),
	CONTRACT_PROGRAM VARCHAR(25),
	TIN_TYPE VARCHAR(15),
	ROLLUP_TIN VARCHAR(12),
	EFFECTIVE_DATE VARCHAR(35),
	CONTRACT_STATUS VARCHAR(12),
	CONTRACT_SIGNED_DATE VARCHAR(35),
	CONTRACT_LENGTH VARCHAR(15),
	QFR_FLAG VARCHAR(5),
	YR_1_BCR_TGT VARCHAR(255),
	YR_2_BCR_TGT VARCHAR(255),
	YR_3_BCR_TGT VARCHAR(255),
	YR_4_BCR_TGT VARCHAR(255),
	YR_5_BCR_TGT VARCHAR(255),
	YR_1_TARGET_ADJUSTMENT_FACTOR VARCHAR(255),
	YR_2_TARGET_ADJUSTMENT_FACTOR VARCHAR(255),
	YR_3_TARGET_ADJUSTMENT_FACTOR VARCHAR(255),
	YR_4_TARGET_ADJUSTMENT_FACTOR VARCHAR(255),
	YR_5_TARGET_ADJUSTMENT_FACTOR VARCHAR(255),
	BASELINE_START_DATE VARCHAR(35),
	BASELINE_END_DATE VARCHAR(35),
	YR_1_START_DATE VARCHAR(35),
	YR_1_END_DATE VARCHAR(35),
	YR_2_START_DATE VARCHAR(35),
	YR_2_END_DATE VARCHAR(35),
	YR_3_START_DATE VARCHAR(35),
	YR_3_END_DATE VARCHAR(35),
	YR_4_START_DATE VARCHAR(35),
	YR_4_END_DATE VARCHAR(35),
	YR_5_START_DATE VARCHAR(35),
	YR_5_END_DATE VARCHAR(35),
	CONTRACT_END_DATE VARCHAR(35),
	EXPENSE_TYPE VARCHAR(255),
	REVENUE_TYPE VARCHAR(20),
	CAP_PERCENT VARCHAR(100),
	CAP_TYPE VARCHAR(20),
	INTERIM_SCD_REQUIRED VARCHAR(40),
	INTERIM_PMT_REQUIRED VARCHAR(40),
	INTERIM_DISCOUNT VARCHAR(50),
	QUALITY_ALIGNMENT VARCHAR(30),
	CIP_PAYMENT VARCHAR(40),
	CIP_START_DATE VARCHAR(35),
	CIP_END_DATE VARCHAR(35),
	CIP_PMPM VARCHAR(50),
	CARE_COORDINATION_PMT VARCHAR(10),
	CARE_COORDINATION_DESC VARCHAR(4000),
	IMPACT_ACCOUNT_ID VARCHAR(300),
	ADDITIONAL_COMMENTS VARCHAR(4000),
	ACTIVE_YEAR VARCHAR(8),
	UPCOMING_SCORECARDS VARCHAR(40),
	UPCOMING_SCORECARD_FLAG VARCHAR(1),
	INSERTED_DATETIME VARCHAR(35),
	UPDATED_DATETIME VARCHAR(35),
	QFR_PRIOR_START_DATE VARCHAR(35),
	QFR_PRIOR_END_DATE VARCHAR(35),
	ADDC_PROGRAM VARCHAR(50),
	REPORTING_END_DATE VARCHAR(35)
);

COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.ACCOUNT_ID IS '"ID" followed by 9 digit code. Sourced from Impact "Title" field, Unique identifier for a specific contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.GROUP_NAME IS 'ACO or Group name.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.REGION IS 'Region associated with the state group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.STATE IS '2-digit abbreviation for State where the group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.COMPANY_DIM_ID IS 'DIM_ID for the state the group is located.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.PROGRAM_DESC IS 'Type of value-based incentive model.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CONTRACT_PROGRAM IS 'Program type is sourced from Impact.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.TIN_TYPE IS 'Defines whether one or multiple TIN(s) are included in the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.ROLLUP_TIN IS '9 digit numerical ID. If single-TIN then this is the TIN used in the contract. If Multi-TIN then this is not necessarily used in the contract measurement rather to be used by accounting when payments are issued.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.EFFECTIVE_DATE IS 'Start of contract, synonomous with YR_1_Start_Date.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CONTRACT_STATUS IS 'Automated Field: Status of contract (active - currently in measurement period of contract, termed - measurement period(s) has ended but scorecards still remain due, Archived - contract has ended and all scorecards/payments have been issued).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CONTRACT_SIGNED_DATE IS 'Date contract was signed.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CONTRACT_LENGTH IS 'Duration (in years) the contract is active for.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.QFR_FLAG IS 'Automated Field: Flag to indicate whether a QFR report should be created for this group. Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_1_BCR_TGT IS 'Target BCR for Year 1 (MP 1) - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_2_BCR_TGT IS 'Target BCR for Year 2 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_3_BCR_TGT IS 'Target BCR for Year 3 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_4_BCR_TGT IS 'Target BCR for Year 4 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_5_BCR_TGT IS 'Target BCR for Year 5 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_1_TARGET_ADJUSTMENT_FACTOR IS 'Target Adjustment factor for PMPM deals in Year 1 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_2_TARGET_ADJUSTMENT_FACTOR IS 'Target Adjustment factor for PMPM deals in Year 2 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_3_TARGET_ADJUSTMENT_FACTOR IS 'Target Adjustment factor for PMPM deals in Year 3 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_4_TARGET_ADJUSTMENT_FACTOR IS 'Target Adjustment factor for PMPM deals in Year 4 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_5_TARGET_ADJUSTMENT_FACTOR IS 'Target Adjustment factor for PMPM deals in Year 5 - entered as a decimal (% format in Excel).';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.BASELINE_START_DATE IS 'Start date of baseline time period - typically 12 months prior to the start of the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.BASELINE_END_DATE IS 'End date of baseline time period - typically 1 day prior to start of contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_1_START_DATE IS 'Start Date of Year 1.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_1_END_DATE IS 'End Date of Year 1.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_2_START_DATE IS 'Start Date of Year 2.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_2_END_DATE IS 'End Date of Year 2.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_3_START_DATE IS 'Start Date of Year 3.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_3_END_DATE IS 'End Date of Year 3.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_4_START_DATE IS 'Start Date of Year 4.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_4_END_DATE IS 'End Date of Year 4.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_5_START_DATE IS 'Start Date of Year 5.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.YR_5_END_DATE IS 'End Date of Year 5.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CONTRACT_END_DATE IS 'Overall end date of contract - typically aligns with maximum end date of years 1-5, with exception of early terminations.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.EXPENSE_TYPE IS 'Expense type used in contract - most Noncat deals limit to $100K per member per year. Even if group doesn''t have financial incentive, please enter the default so we can utilize in other reporting.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.REVENUE_TYPE IS 'Revenue type used in contract. Net revenue is standard. Even if group doesn''t have financial incentive, please enter the default so we can utilize in other reporting.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CAP_PERCENT IS 'Payment Cap % - represents the max amount a group can be paid, entered as a decimal (e.g. 5% of medical expense). Leave blank if N/A.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CAP_TYPE IS 'Type of cap used - sourced from contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.INTERIM_SCD_REQUIRED IS 'Does contract require interim scorecards? Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.INTERIM_PMT_REQUIRED IS 'Does contract require interim payments? Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.INTERIM_DISCOUNT IS 'Interim Discount per contract (Usually 75% if there is one) - Entered as a decimal that aligns with the % that we will pay out.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.QUALITY_ALIGNMENT IS 'Does quality measurement period align with shared savings measurement period? If yes, Shared Savings, If no, Calendar Year.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CIP_PAYMENT IS 'Does contract have CIP payments? Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CIP_START_DATE IS 'Start date of Measurement period for CIP. Leave blank if N/A.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CIP_END_DATE IS 'End date of Measurement period for CIP. Leave blank if N/A.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CIP_PMPM IS 'CIP PMPM payment amount, entered as a number.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CARE_COORDINATION_PMT IS 'Does contract have a care coordination payment? Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.CARE_COORDINATION_DESC IS 'Notes/description of care coordination payment and length. Leave blank if does not apply.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.IMPACT_ACCOUNT_ID IS 'Links a group''s contracts across multiple contracts - sourced from the Impact field "Account_ID".';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.ADDITIONAL_COMMENTS IS 'Any notes/comments/unique terms of contract (4000 characters max for cell). Leave blank if does not apply.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.ACTIVE_YEAR IS 'Automated field: Lists the year that is currently active as of today''s date.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.UPCOMING_SCORECARDS IS 'Automated Field: Lists the applicable scorecards that are due in the coming months.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.UPCOMING_SCORECARD_FLAG IS 'Automated Field: Flag to indicate whether a contract has scorecards due in the coming months. Yes - Y, No - N.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.QFR_PRIOR_START_DATE IS 'Automated Field: For groups where the baseline period is not immediately before the start of the contract, this date reflects the date 12 months before the start of the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.QFR_PRIOR_END_DATE IS 'Automated Field: For groups where the baseline period is not immediately before the start of the contract, this date reflects the date 1 day before the start of the contract.';
COMMENT ON COLUMN ETL.SMART_ACO_CONTRACTS_L.ADDC_PROGRAM IS 'Is contract part of the ADDC program (risk adjustment)? Yes- Y, No-N, R-Reports only.';

