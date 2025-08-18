

  ##Start  of Employer mbgcounties Test case
@test
  Feature: Feature1
Scenario: Verify if the data is loaded correctly1
Given allgroups extract data is loaded in hdfs path1
When I need to check if the allgroups files are populated correctly1
Then The result should be true,The record count of allgroups is greater than zero1