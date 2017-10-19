This is a java implementation of a map reduce program which for a given matrix outputs its transpose.
Commands to use on hadoop cluster:
- define the input file: (the input file have the following path input/test.csv

hdfs dfs -put input/test.csv input

- execute the jar 
hadoop jar mr_test-1.0-SNAPSHOT.jar DriverTP input/input.csv output

- copy output files to home directory:

hdfs dfs -get output output

- transform result into .csv file since the result is comma separated:

cat output/part-r-* > result.csv
