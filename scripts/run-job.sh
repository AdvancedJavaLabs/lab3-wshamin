#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$PROJECT_DIR/docker"

NUM_MAPPERS=${1:-2}
NUM_REDUCERS=${2:-1}

echo "=========================================="
echo "Запуск MapReduce job для анализа продаж"
echo "=========================================="
echo "Mappers: $NUM_MAPPERS"
echo "Reducers: $NUM_REDUCERS"
echo ""

HADOOP_CONTAINER=$(docker ps --format "{{.Names}}" | grep -E "(hadoop-namenode|hadoop-single)" | head -1)

if [ -z "$HADOOP_CONTAINER" ]; then
    echo "Hadoop не запущен. Запускаем кластер..."
    cd "$DOCKER_DIR"
    docker compose up -d 2>/dev/null || docker-compose up -d
    echo "Ожидание запуска Hadoop (30 секунд)..."
    sleep 30
    
    HADOOP_CONTAINER=$(docker ps --format "{{.Names}}" | grep -E "(hadoop-namenode|hadoop-single)" | head -1)
    if [ -z "$HADOOP_CONTAINER" ]; then
        echo "Ошибка: не удалось запустить контейнер Hadoop"
        exit 1
    fi
fi

echo "Используется контейнер: $HADOOP_CONTAINER"

echo "Сборка проекта..."
cd "$PROJECT_DIR"
gradle clean build -x test

JAR_FILE=$(find "$PROJECT_DIR/build/libs" -name "*.jar" -not -name "*-sources.jar" -not -name "*-javadoc.jar" | head -1)
if [ -z "$JAR_FILE" ] || [ ! -f "$JAR_FILE" ]; then
    echo "Ошибка: JAR файл не найден в build/libs/"
    exit 1
fi
echo "Найден JAR: $JAR_FILE"

echo "Копирование JAR в контейнер..."
docker cp "$JAR_FILE" "$HADOOP_CONTAINER:/tmp/sales-analysis.jar"

echo "Копирование CSV файлов в контейнер..."
TEMP_DIR="/tmp/csv-input"
docker exec "$HADOOP_CONTAINER" bash -c "rm -rf $TEMP_DIR && mkdir -p $TEMP_DIR"
for csv_file in "$PROJECT_DIR"/*.csv; do
    if [ -f "$csv_file" ]; then
        docker cp "$csv_file" "$HADOOP_CONTAINER:$TEMP_DIR/"
    fi
done

echo "Подготовка HDFS..."
docker exec "$HADOOP_CONTAINER" bash -c "
    # Создаем директории в HDFS
    hdfs dfs -mkdir -p /input /output || true
    
    # Удаляем старые результаты
    hdfs dfs -rm -r -f /output/sales-analysis || true
    
    # Копируем CSV файлы в HDFS
    echo 'Копирование CSV файлов в HDFS...'
    hdfs dfs -put $TEMP_DIR/*.csv /input/ 2>/dev/null || true
    hdfs dfs -ls /input/
    
    # Очищаем временную директорию
    rm -rf $TEMP_DIR
"

echo ""
echo "Запуск MapReduce job..."
START_TIME=$(date +%s)

docker exec "$HADOOP_CONTAINER" bash -c "
    hadoop jar /tmp/sales-analysis.jar \
        com.itmo.lab3.SalesAnalysisJob \
        /input \
        /output/sales-analysis \
        $NUM_MAPPERS \
        $NUM_REDUCERS
"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "Job завершен за $ELAPSED секунд"

echo ""
echo "Копирование результатов из HDFS..."
docker exec "$HADOOP_CONTAINER" bash -c "hdfs dfs -cat /output/sales-analysis/part-r-*" > "$PROJECT_DIR/results/raw-results.txt"

echo ""
echo "Форматирование результатов..."
echo -e "Category\t\t\tRevenue\t\t\tQuantity" > "$PROJECT_DIR/results/sales-results.txt"

cat "$PROJECT_DIR/results/raw-results.txt" | \
    awk -F',' '{printf "%15.2f|%-20s|%10s\n", $2, $1, $3}' | \
    sort -rn | \
    awk -F'|' '{printf "%-20s %15.2f %10s\n", $2, $1, $3}' >> "$PROJECT_DIR/results/sales-results.txt"

echo ""
echo "=========================================="
echo "Результаты сохранены в: results/sales-results.txt"
echo "=========================================="
cat "$PROJECT_DIR/results/sales-results.txt"
