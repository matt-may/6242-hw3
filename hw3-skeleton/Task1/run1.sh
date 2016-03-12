hadoop jar ./target/task1-1.0.jar edu.gatech.cse6242.Task1 /user/me/graph1b.tsv /user/me/task1output1
hadoop fs -getmerge /user/me/task1output1/ task1boutput1-2.tsv
hadoop fs -rm -r /user/me/task1output1/
