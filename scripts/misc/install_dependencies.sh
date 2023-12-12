#!/bin/bash
set -x
cd /opt

wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xf spark-3.5.0-bin-hadoop3.tgz
rm spark-3.5.0-bin-hadoop3.tgz

wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar xf kafka_2.13-3.6.1.tgz
rm kafka_2.13-3.6.1.tgz

wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar xf hadoop-3.3.6.tar.gz
rm hadoop-3.3.6.tar.gz
cat > hadoop-3.3.6/etc/hadoop/core-site.xml <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://spark-test1:9000</value>
    </property>
</configuration>
EOF
cat > hadoop-3.3.6/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/data/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/data/hdfs/datanode</value>
    </property>
</configuration>
EOF
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64' >> hadoop-3.3.6/etc/hadoop/hadoop-env.sh
