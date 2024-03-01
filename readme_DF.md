# Tutorial PySpark y AWS: _DataFrames_

Indice

1. [Introduccion](#1.-Introduccion)
2. [Spark Working with DataFrames](#2.-spark-working-with-dataframes)
    - [Crear un DataFrame](#.-crear-un-dataframe)
    - [Spark Infer Schema](#.-spark-infer-schema)
    - [Spark **kwargs options](#.-spark-usar-**kwargs)
    - [Spark DF from RDDs](#.-spark-df-from-rdds)
    - [Spark Select DF Columns](#.-spark-select-df-columns)


## 1. Introduccion

__DataFrames__ son un wrapper de los __rdd__, son como una representacion de una tabla con solumnas y file. 
Los rdd tienen ciertas limitaciones como la __falta de esquema__

Los __DataFrames__ se pueden formar a partir de:
+ Structured data files.
+ Unstructured data files
+ External Sources
+ Existing RDDs.

__Un DataFrame es un DataSet organizado en columnas con nombres__


## 2. Spark working with DataFrames

Indirectamente los __dataframes__ trabajan con los __RDDs__ asique no hayq ue manipular nada extra.
Lo primero que hay que considerar es que cuando trabajamos con __RDDs__ leemos o escribimos los datos de un __SparkContext__. Si leemos del __sparkContext__ spark lo que nos dá es un __RDD__.
Pero si leemos los datos de una __SparkSession__ entonces spark nos da los datos en un __DataFrame__.
Esta es la diferencia entre crear un __RDD__ o un __DataFrame__.

### Crear un DataFrame


```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').master('local[*]').getOrCreate()
```

+ __getOrCreate()__ Hay una limitación que solo se puede crear una __SaprkSession__ a la vez. Asique o usamos una creada o creamos una nueva.

+ __appName()__ Es el nombre que le vamos a poner a la __SparkSession__

```python
PATH = "/mnt/d/Proyectos/Tutorial-SparkAWS/data/StudentData.csv"
df = spark.read.csv(PATH)
```

Para poder consultar este __dataframe__ es necesario ejecutar una __accion__ sobre el mismo. Si fuera un __rdd__ usariamos __collect()__ pero sobre un __dataframe__ usamos __show()__ o __head()__

```python
df.show(5)
```

### Spark infer Schema

+ Hay dos formas de indicar el __schema__ de un DataFrame.
1. Que Spark lo infiera al momento de leer el __DataSet__
2. Especificarlo como un __parametro de entrada__

Del ejemplo anterior mostramos el __Esquema__

```python
df.printSchema()
```

```
Vemos que todo es String, porque no le especificamos un esquea¿ma o que lo infiera.
```

1. Inferir esquema.

```
Spark no es muy preciso para inferir esquemas, pero es de utilidad para tener una idea de lo que tenemos en el DataSet.
```

```python
df = spark.read.oprtion("Header","True").option("inferSchema", True).csv(PATH)
df.printSchema()
```

2. Especificar Esquema.


```
Si no queremos que Spark infiera el esquema podemos especificarlo.
Para esto usamos __StructType__ y __StructField__
```

+ __StructType__ Lo usamos para generar el esquema, tiene la metadata de cada columna.
+ __StructField__ Es la metadata de cada columna, especifica el tipo.

```python

from pyspark.sql.types import StructType, StructField, StringType, IntegerType 

schema = StructType ([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("role", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True),
])

df = spark.read.schema(schema).option("header",True).csv(PATH)
df.show(5)
```

__IMPORTANTE__ los nombres de las columnas deben coincidir


### Spark usar _**kwargs_

```
Aveces necesitamos concatenar muchos ..option().option.. y puede hacer que el codigo no sea legible.
Podemos solucionar esto usando **kwargs donde debemos especificar el nombre de la option = valor
```

__IMPORTANTE__ notar que no usamos __.option()__ sino __.options()__


```pyhton
df = spark.read.options(inferSchema=True, Header=True, delimiter=",").csv(PATH)
df.printSchema()
```

### Spark DF from RDDs

```
Vamos a crear un DataFrame a partir de un RDD usando el DataSet de Alumnos.csv usando la funcion .toDF()
```

1. Creamos el RDD

```python
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('Test_rdd').setMaster("local[*]")
spark_rdd = SparkContext.getOrCreate(conf=conf)
```

2. Preparamos el __RDD__ separadando las columnas.

```python
PATH = "/mnt/d/Proyectos/Tutorial-SparkAWS/data/StudentData.csv"

rdd = spark_rdd.textFile(PATH)
header = rdd.first()
rdd_data = rdd.filter(lambda x: x != header)
#print(rdd_data.count())

rdd_data_f = rdd_data.map(lambda x:x.split(","))
rdd_data_f = rdd_data_split.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]])
rdd_data_f.first()
```

3. Convertimos a DF

```python
lista_headers = header.split(",")
df = rdd_data_f.toDF(lista_headers)
```

__EL PROBLEMA CON ESTE ENFOQUE ES QUE NO TIENE TIPOS DE DATOS O ESQUEMA__

+ Creamos le esquema

```
Si vamos a especificar el esquema, en lugar de usar .toDF() usamos spark.createDataFrame(rdd, schema=)
```

```python
schema_rdd = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("email", StringType(), True)
])

mi_df = spark.createDataFrame(rdd_data_f, schema=schema_rdd)
print(mi_df.printSchema())
```

__IMPORTANTE__ si al momento de crear el DF usando la transformacion __spark.createDataFrame()__ recibimos el error:

```
TypeError: field age: IntegerType() can not accept object '28' in type <class 'str'>
```

es porque debemos castear los tipos de datos al momento de hacer el __split al crear el rdd__ esto es porque __StructType__ funciona cuando necesitamos espeficiar los tipos de fuentes externas como __.csv__ pero no funciona para inferir de un __rdd__ por eso agregamos esto a la lectura del __rdd__

```pyhton
rdd_data_f = rdd_data_split.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]])
```

### Spark select DF Columns

Podemos espeficiar las columnas que queremos ver al momento de seleccionar un DF.

1.

```python
df.select("col1", "col2")
```

+ Select es una __transformacion__ peara ver realmente el DataFrame debemos usar una accion como __show__

2. Otra forma es usando __dot notation__

```python
df.select(df.age, df.email).show(2)
```

Ambas formas hacen lo mismo y no hay un beneficio de usar una u otra.

3. Usando la __funcion col__ de pyspark.sql.functions

```python
from pyspark.sql.functions import col

df.select(col("age"), col("gender")).show()
```

4. Mostrar todas las columnas __*__

```python
df.select("*").show(4)
```

5. seleccionar columnas usando __index__

```python
df.select(df.columns[3:]).show(4)
```

6. Combinando todo lo anterior

```python
df.select(col("age"), df.columns[-1]).show(4)
```

