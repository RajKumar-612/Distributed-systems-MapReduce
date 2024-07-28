How to Run:

Step 1: Compile the Program

Navigate to your project directory containing the pom.xml file. Compile the program and create a JAR file using Maven.

mvn package

Step 2: Copy the JAR to the Namenode Container

Copy the generated JAR file into the namenode container of the Hadoop cluster.

docker cp target/MapReduce-1.0-SNAPSHOT.jar namenode:/

Step 3: Copy Data to HDFS

Ensure the input data is available in HDFS. Copy the data file into the namenode container.

docker cp df_file.txt namenode:/

Enter the namenode container and copy the data from the local filesystem to HDFS:

docker exec -it namenode bash
hdfs dfs -mkdir -p /user/input
hdfs dfs -put df_file.txt /user/input/
exit

Step 4: Run the Program

Run the MapReduce program with Hadoop. Ensure you are still within the namenode container or re-enter it if you've exited. Execute the Hadoop job:

hadoop jar MapReduce-1.0-SNAPSHOT.jar org.example.CharacterCounter /user/input/df_file.txt /user/output_char

hadoop jar MapReduce-1.0-SNAPSHOT.jar org.example.InvertedIndex /user/input/df_file.txt /user/output_Indexer

hadoop jar MapReduce-1.0-SNAPSHOT.jar org.example.KMeansClusteringHadoop /user/input/rainfall.txt /user/output_kmeans

Step 5: Check the Output

After the job completes, check the output with the following HDFS command:

hdfs dfs -cat /user/output_kmeans/part-r-00000 # for kmeans program

hdfs dfs -cat /user/output_char/part-r-00000 # for character count program

hdfs dfs -cat /user/output_Indexer/part-r-00000 # for the inverted index program

This displays the output of each program.
