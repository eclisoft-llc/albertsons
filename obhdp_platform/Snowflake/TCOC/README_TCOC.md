# TCOC layer 
## GitHub
<p>This folder stores the necessary files to create the views exposed to the consumers for Medical and RX claims, and views to data from external systems</p>
<p>The views in this layer, are enforcing the security required for PHI/PII where only specific roles can see the fields identified as personal information.</p>

###Views
<p>Contains the DDL for the views in Snowflake:</p>
<p>Common format views for Medical and RX claims, security implemented, and point to a DM table to ensure no disruptions to the business during the load process</p>
<p>Provider views, security implemented</p>
<p>IMDM views, security implemented</p>

### TCOC layer flow
![high level TCOC diagram](../../img/TCOC_DataMarts_Process.jpg)

