## Installing, building ans testing 

```
make all
```
We do not bundle `pyspark` via dependencies but expect it to be present globally. To make it being picked up by IDE 
for autocomplete add path to pyspark module manually to project's sources. 

For example for PyCharm and Spark 3.5.0 installed on Mac via brew (brew install apache-spark): 

PyCharm > Settings > Project Structure > Add Content Root: /usr/local/Cellar/apache-spark/3.5.0/libexec/python/lib/pyspark.zip 

Running tests requires Spark 3.5.0 and higher. For building separately run:

```
make dev_deps deps build
``` 

## Running (locally)

```
spark-submit --py-files dist/jobs.zip,dist/libs.zip dist/main.py --job marketing_data_intake
``` 

## Report produced based on the example csv

```
+---+-----+--------+----------+---------+----------+
|id |stock|price   |date      |validated|updated_by|
+---+-----+--------+----------+---------+----------+
|1  |A    |10.00000|2024-01-01|1        |3         |
|3  |A    |9.00000 |2024-01-01|1        |7         |
|4  |A    |11.00000|2024-01-02|1        |NULL      |
|5  |B    |45.00000|2024-01-01|1        |NULL      |
|6  |B    |50.00000|2024-01-02|1        |NULL      |
|7  |A    |10.00000|2024-01-01|1        |8         |
|8  |A    |9.00000 |2024-01-01|1        |NULL      |
+---+-----+--------+----------+---------+----------+
```

## Things to be improved:

1. Adding full-fledged e2e/integration tests running in docker
2. Creating helm chart distribution for a Kubernetes-based deployment 
3. Re-writing to Java or Scala to bring domain types into the scope :)

---------

Boilerplate is created based on:

https://github.com/ekampf/PySpark-Boilerplate
