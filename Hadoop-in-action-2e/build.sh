# Copyright Manning Publications 2014-2015

HADOOP_HOME=/home/hadoop/hadoop-2.4.1

CLASSPATH=$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.4.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/lib/commons-io-2.4.jar:$HADOOP_HOME/share/hadoop/common/lib/hadoop-annotations-2.4.1.jar

# Chapter 1 Example
CHAPTER=Chapter1
javac -cp $CLASSPATH -d $CHAPTER/build $CHAPTER/WordCount.java

# Chapter 4 Examples
CHAPTER=Chapter4
javac -cp $CLASSPATH -d $CHAPTER/build $CHAPTER/MyJob.java
javac -cp $CLASSPATH -d $CHAPTER/build $CHAPTER/AverageByAttributeMapper.java
javac -cp $CLASSPATH -d $CHAPTER/build $CHAPTER/CitationHistogram.java

# Chapter 6 Examples
CHAPTER=Chapter6
javac -cp $CLASSPATH -d $CHAPTER/build $CHAPTER/AverageByAttributeMapper.java
