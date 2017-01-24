# SIEMENS_odspark
File: ./data_format_clean.sh Removes the trailing spaces and changes the delimiter to comma. 

File. spark_outliers_detect.scala
Steps done in that file. 

1. Convert the date column of String type to Date datatype. 
2. Do a group by on Date with  mean of Values column
3. Fit the regression on Date and Value. 
4. With Slope and Intercept find the Deivation. 
5. Take the top k=2 surprise values. (Append in a list for all files in the folder)
6. DO MAD on it and detect the outliers. 

