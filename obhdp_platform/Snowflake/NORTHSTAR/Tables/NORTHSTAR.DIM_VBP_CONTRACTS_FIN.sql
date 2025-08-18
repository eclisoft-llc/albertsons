Create TABLE if not exists  NORTHSTAR.DIM_VBP_CONTRACTS_FIN
(Type VARCHAR(15),
Impact_ID VARCHAR(15),
Contract_Entity VARCHAR(100),
TaxID varchar(20),
ContractStart varchar(23),
ContractEnd	varchar(23),
Assigned_Members number(25)
)