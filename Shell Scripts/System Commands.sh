# List of used commands to remeber
-------------------------------------------------------------------
start-dfs.sh
start-yarn.sh
# Stop HDFS
stop-dfs.sh
stop-yarn.sh

source myenv/bin/activate
cd /usr/local/zeppelin
./bin/zeppelin-daemon.sh start
./bin/zeppelin-daemon.sh restart
./bin/zeppelin-daemon.sh stop
------------------------------------------------------------------

hdfs dfs -mkdir /user/thokozile/images/Spring
hdfs dfs -mkdir /user/thokozile/images/Summer
hdfs dfs -mkdir /user/thokozile/images/Winter
hdfs dfs -mkdir /user/thokozile/images/Fall
hdfs dfs -mkdir /user/thokozile/images/Other
hdfs dfs -mkdir /user/thokozile/metadata

hdfs dfs -put /media/sf_Data/Spring/* /user/thokozile/images/Spring/
hdfs dfs -put /media/sf_Data/Summer/* /user/thokozile/images/Summer/
hdfs dfs -put /media/sf_Data/Winter/* /user/thokozile/images/Winter/
hdfs dfs -put /media/sf_Data/Fall/* /user/thokozile/images/Fall/
hdfs dfs -put /media/sf_Data/Other/* /user/thokozile/images/Other/
hdfs dfs -put /media/sf_Data/meta_data_full.csv /user/thokozile/metadata/

#list folders in hdfs
hdfs dfs -ls /user/thokozile/images

#check job output:
hdfs dfs -ls /user/thokozile/output
hdfs dfs -ls /user/thokozile/outputMetaData

#view output part file:
hdfs dfs -cat /user/thokozile/output/Spring/part-r-00000
hdfs dfs -cat /user/thokozile/output/Summer/part-r-00000
hdfs dfs -cat /user/thokozile/output/Winter/part-r-00000
hdfs dfs -cat /user/thokozile/output/Fall/part-r-00000
hdfs dfs -cat /user/thokozile/output/Other/part-r-00000
hdfs dfs -cat /user/thokozile/outputMetaData/part-r-00000
-------------------------------------------------------------------------------
cd eclipse-workspace/

# Empty previous compiled jar files
rm -r bin/*

# Recompile Jar Files
javac -source 1.8 -target 1.8 -classpath $(hadoop classpath) -d bin src/ImageMapper.java src/ImageReducer.java src/ImageProcessingJob.java
javac -source 1.8 -target 1.8 -classpath $(hadoop classpath) -d bin src/ProductMapper.java src/ProductReducer.java src/ProductJob.java

#create jar file
jar -cvf imageprocessing.jar -C bin .    
jar -cvf metadataprocessing.jar -C bin .    

----------------------------------------------------------------------------------
#deleted previous output files :
hdfs dfs -rm -r /user/thokozile/output
hdfs dfs -rm -r /user/thokozile/outputMetaData

hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar ImageProcessingJob /user/thokozile/images/Spring /user/thokozile/output/Spring
hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar ImageProcessingJob /user/thokozile/images/Summer /user/thokozile/output/Summer
hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar ImageProcessingJob /user/thokozile/images/Winter /user/thokozile/output/Winter
hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar ImageProcessingJob /user/thokozile/images/Fall /user/thokozile/output/Fall
hadoop jar /home/thokozile/eclipse-workspace/ImageProcessing/imageprocessing.jar ImageProcessingJob /user/thokozile/images/Other /user/thokozile/output/Other
hadoop jar /home/thokozile/eclipse-workspace/MetaDataProcessing2/metadataprocessing.jar ProductJob /user/thokozile/metadata /user/thokozile/outputMetaData

-----------------------------------------------------------------------------------

