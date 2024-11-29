# report-generation

service port : 8080 

(Client)calling dmnservice  :http://localhost:8081/Decision


Run
====>mvn clean compile quarkus:dev -DskipTests

Request
===> upload csv file in requests folder 

i have created a sample file a.csv attached upload in  folder---> /requests 


Response
===>in /data folder two  files Error and Success reports 

will be generated as csv with Slfe and  extra columns as 
ReasonID
IsError
IsCorrect
ErrorIds
AuditTrails


large xlsx file reading reference

https://github.com/monitorjbl/excel-streaming-reader/issues/73
https://github.com/monitorjbl/excel-streaming-reader