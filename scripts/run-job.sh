#!/bin/bash

cd docker
docker compose up -d
sleep 30
cd ..
gradle clean build -x test
sleep 30
docker cp build/libs/lab3-wshamin-1.0-SNAPSHOT.jar hadoop-namenode:/tmp/sales-analysis.jar
docker exec hadoop-namenode mkdir -p /tmp/csv-input
docker cp 0.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 1.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 2.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 3.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 4.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 5.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 6.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker cp 7.csv hadoop-namenode:/tmp/csv-input/
sleep 3
docker exec hadoop-namenode hdfs dfs -rm -r -f /input /output
docker exec hadoop-namenode hdfs dfs -mkdir /input /output
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/0.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/1.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/2.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/3.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/4.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/5.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/6.csv /input
docker exec hadoop-namenode hdfs dfs -put /tmp/csv-input/7.csv /input
docker exec hadoop-namenode hadoop jar /tmp/sales-analysis.jar \
  com.itmo.lab3.SalesAnalysisJob \
  /input \
  /output/sales-analysis \
  2 \
  1
sleep 30
mkdir -p results
docker exec hadoop-namenode sh -c 'hdfs dfs -cat /output/sales-analysis/part-r-*' > results/raw-results.txt
echo -e "Category\t\t\tRevenue\t\t\tQuantity" > results/sales-results.txt
grep -E '^[^,]+,[0-9]+\.[0-9]+,[0-9]+$' results/raw-results.txt | \
awk -F',' '{printf "%-20s %15.2f %10s\n", $1, $2, $3}' >> results/sales-results.txt
