#HW1
#####requirements
- Cloudera QuickStart VMs CDH 5.12
- 64-bit host OS and a virtualization product that can support a 64-bit guest OS
- 8+ GiB RAM
#####build
`mvn clean package`
#####prepare input
`prepare.sh my_file_path hdfs_working_dir`
#####run
`run.sh hdfs_working_dir`
#####view output
`view.sh hdfs_working_dir`
#####links
https://docs.microsoft.com/ru-ru/azure/hdinsight/hdinsight-develop-deploy-java-mapreduce-linux

https://examples.javacodegeeks.com/enterprise-java/apache-hadoop/hadoop-sequence-file-example

https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
