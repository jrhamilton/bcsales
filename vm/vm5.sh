#!/bin/bash


mkdir spark
cd spark

echo "INSTALL JAVA"
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz

tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz

export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
echo "export JAVA_HOME=\"${HOME}/spark/jdk-11.0.2\"
export PATH=\"${JAVA_HOME}/bin:${PATH}\""  >> $HOME/.bashrc

echo "=== CHECK JAVA VERSION ==="
java --version

rm openjdk-11.0.2_linux-x64_bin.tar.gz


echo "INSTALL SPARK"
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz

export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
echo "export SPARK_HOME=\"${HOME}/spark/spark-3.3.2-bin-hadoop3\"
export PATH=\"${SPARK_HOME}/bin:${PATH}\"" >> $HOME/.bashrc

echo "SET PySpark"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
echo "export PYTHONPATH=\"${SPARK_HOME}/python/:$PYTHONPATH\"
export PYTHONPATH=\"${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH\"" >> $HOME/.bashrc

PYSPARK_PYTHON="/usr/bin/python"
PYSPARK_DRIVER_PYTHON="/usr/bin/python"
echo "PYSPARK_PYTHON=\"/usr/bin/python\"
PYSPARK_DRIVER_PYTHON=\"/usr/bin/python\"" >> $HOME/.bashrc

wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

cd $HOME/
