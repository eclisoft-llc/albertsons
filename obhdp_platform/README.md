# TCOC 
<p>This repository is used for TCOC Data Loads. Scripts are created to move the data from SMART to SnowFlake and load data using automated filewatcher into Snowflake STAGE/ETL & Foundation layer.</p>
<p>After the load of external systems is complete, the final step is to convert the claims into Med/RX common format tables.</p>
<p>The TCOC layer implement the security for PHI/PII data, and integrates with Enterprise Individual and Enterprise Provider.</p>


### Repository Access
Raise a secure request to obhdp_github_read/obhdp_github_write group for the read/write access.


### TCOC
![high level THO diagram](./img/THO_Data_Layers.jpg)


![high level flow diagram](./img/TCOC_Flow-Option-2.jpg)

