This is a java implementation of a map reduce program which for a given matrix outputs its transpose.
Commands to use on hadoop cluster:
- define the input file:
hdfs dfs -put etc/hadoop input/test.csv 
- execute the jar 
hadoop jar mr_test-1.0-SNAPSHOT.jar DriverTP input output
