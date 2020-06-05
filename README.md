# Projet Spark Robin SAINT GEORGES 5ALJ

## to init :

check that you have python, java, spark and pyspark installed
exec from exo directory :

```
python setup.py bdist_egg
```

## to launch :

exec from exo directory :
```
/usr/local/spark-X.X.X-preview2-bin-hadoopX.X/bin/spark-submit --master local --py-files dist/HelloWorld-0.1-py2.7.egg launch.py /path/to/file.csv
```
