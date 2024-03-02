# Tutorial PySpark y AWS: _DataFrames_

Indice

1. [Introduccion](#1.-Introduccion)
2. [Spark Working with DataFrames](#2.-spark-working-with-dataframes)
    - [Crear un DataFrame](#.-crear-un-dataframe)
    - [Spark Infer Schema](#.-spark-infer-schema)
    - [Spark **kwargs options](#.-spark-usar-**kwargs)
    - [Spark DF from RDDs](#.-spark-df-from-rdds)
    - [Spark Select DF Columns](#.-spark-select-df-columns)
    - [Spark withColunn](#.-spark-withcolumn)
    - [Spark withColumnRename](#.-spark-withcolumnrename)
    - [Spark DF Filter/Where](#.-spark-df-filter/where)
        - __isin()__
        - __startswith()__
        - __endswith()__
        - __contains()__
        - __like()__
    - [Spark ejercicio rapido](#.-spark-ejercicio-rapido)
    - [spark SQL](#.-spark-sql)
        - __Count()__
        - __Distinct()__
        - __Duplicate()__
    - [Spark ejercicio rapido](#.-spark-ejercicio-rapido)
    - [Spark orderby/sort](#.-spark-orderby/sort)
    - [Spark groupBY](#.-spark-groupBy)
        - __count()__
        - __sum()__
        - __max()__
        - __min()__
        - __avg()__
    - [Spark groupBy & Filtering](#.-Spark-groupBy-&-Filtering)
    - [Spark ejercicio rapido](#.-spark-ejercicio-rapido)


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

### Spark withColumn

```
Esta transformacion se usa para manipular el valor de una columna.
Por ejemplo si tenemos un DF con 100 columnas pero solo nos interesa cambiarle el tipo de dato a una, no tiene sentido crear un StructType.
En su lugar podemos usar un .withColumn() y manipular el valor de la columna directamente.
```
1. Casteos

```python
from pyspark.sql.functions import col

df.withColumn("roll2", col("roll").cast("Integer")).show(5)
```

En este ejemplo vamos a ver que se crea una nueva columna __roll2__ y va a tomar la columna __roll__ y castearla a __Integer__
Si hubieramos especificado el mismo nombre __roll__ la columna se sobrescribe.

2. Manipulacion de valores

```python
from pyspark.sql.functions import col

df.withColumn("marks2", col("marks") + 5).show(5)
```

Esto lo que hace es agregarle 5 a cada registro creando una nueva columna __marks2__


3. Crear nueva columna con valor Hardcode

Para poder crear un valor __fijo__ o __constante__ debemos usar el tipo __lit__ o literal que nos permite insertar un valor fijo en la transformacion __withColumn()__

```python
from pyspark.sql.functions import col, lit

df.withColumn('pais',lit('ARG')).show(5)
```

__si no se especifica lit(), tendriamos un error de tipos__ Si necesitamos modificar una columna por un valor fijo, tambien usamos __lit()__

4. Concatenacion de .withColumn()

```
Si necesitamos manipular varias columnas lo podemos hacer concatenando o usnado .dot notation, teniendo en cuenta que lo que se va encadenando es el DF modificado por los pasos anterior.
```

```python
from pyspark.sql.functions import col

df.withColumn('marks2', col('marks') - 10).withColumn('marks2', col('marks') + 20)).show(5)
```

_En este ejemplo si en el DF original marks tenia el valor 59, primero le resta 10, quedando en 49 y luego le suma 20, dejando el resultado en 69._

### Spark withColumnRename

1. withColumnRename

```
Es una transformación que se usa para cambiar el nombre de una columna.
Para que el cambio de nombre se haga efectivo debemos asignarlo a un DataFrame
```

```python
df = df.withColumnRename("gender", "sex").withColumnRename("roll","roll_number")
```

2. Alias

Tambien se puede cambiar el nombre de una columna en __runtime__ y no a nivel de DF. Para esto combinamos __.select(col("columna").alias())__

```python
from pyspark.sql.functions import col

df.select(col("age").alias("Edad")).show()
```

### Spark DF Filter/Where

Se usa para filtrar resultados. Se puede usar tanto __filter__ como __where__

1. Condición simple.

```python
from pyspark.sql.functions import col

df.filter(col("course")=='DB').show(5)
df.filter(df.course == "DB").show(5)
```

2. Condición Multiple.

```python
df.filter(
            (df.course == "DB") & (df.marks > 50)
        ).show(5)
```

1. Condicion multiple con __isin__

Para checkear condiciones múltiples podemos usar __isin__.
Podemos crear una lista con las condiciones que nos iteresan.

```python
from pyspark.sql.functions import col

l_courses = ["DB", "Cloud", "OOP"]
df.filter(col("course").isin(l_courses))
```

4. Filtros sobre __Strings__ con __.startswith()__ y __.endwith()__

Usamos esta transformacion para checkear que una cadena comienza o termina con determinada letra o caracter.

```python
from pyspark.sql.functions import col

df.filter(df.course.startswith("D")).show(2)
df.filter(col("course").startswith("D")).show(2)
```

5. Filtros con __.contains()__ 

Lo usamos para determinar si un String o cadena contiene otro __string__

```python
from pyspark.sql.functions import col

df.filter(df.name.contains("Eleno")).show(2)
df.filter(col("name").contains("Eleno")).show(2)
```

Otra forma de usar __.contains()__ es con __like__

```python
from pyspark.sql.functions import col

df.filter(df.name.like("%Ele%")).show(2)
df.filter(col("name").like("%Ele%")).show(2)
```

### Spark ejercicio rapido

Para este ejecucion vamos a usar el DataSet __StudentsData.csv__

1. Crear una nueva columna __total marks__ con el valor 120
2. Crear una columna __total mark avg__ para cada alumno con la formula 
__(marks/total_marks)*100__
3. Filtrar los estudiantes con AVG > 80% in __OOP__
4. Filtrar los estudiantes con AVG > 60% in __CLOUD__
5. Mostrar los nombres y notas de __3__ y __4__

__Ver solucion en notebook 9. Spark Ejercicio rápido__


### Spark SQL

1. __.Count()__

```
Es una accion que se usa sobre un DataFrame o sobre una transformacion. La usamos para obtener el numero total de registros o total de registros sobre una condición.
No se puede combinar con .show()
```

```python
from pyspark.sql.functions import col

df.count()
df.filter(col("age")>18).count()
```

2. __.Distinct()__

```
Es una transformacion que se usa para mostrar registros unicos.
```

```python
from pyspark.sql.functions import col

df.distinct().show()
df.select("gender", "age").distinct.count()
```

El ultimo ejemplo deberia mostrar la cantidad de registros únicos para la combinacion __genero__ y __edad__

3. __.dropDuplicates()__ 

Es usa para obtener el primer registro único segpun una seleccion de columnas.

```python
df.dropDuplicates(["gender","age"]).show()
df.dropDuplicates(["age"]).show()
```

### Spark ejercicio rapido

```
Para este ejercicio usamos StudentData.csv.
Se pide escribir un codigo que muestre todos los registros unicos por "age", "gender" y "couser".
```

```python
from pyspark.sql import SparkSession

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("roll", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("emails", StringType(), True),
])

df = spark.read.options(schema=schema, header=True).csv("/mnt/d/Proyectos/Tutorial-SparkAWS/data/StudentData.csv")

df.dropDuplicates(["gender", "age", "course"]).show(4)
```

Mismo resultado.

```python
df2 = df.select("age", "gender", "course").distinct()
df2.count()
```

### Spark orderBY/sort

```
Al igual que en SQL .sort() se usa para ordernar el resultado de un DataFrame según determinada columna o varias. Tambien podemos usar .orderBy().
Por defecto el orden es ascendente.
```

1. De a una columna

```python
df.sort(df.age.asc()).show()
```

2. De a dos o mas columnas.

```python
df.sort(col('age').asc(), col('gender').desc()).show()
```

__Para especificar las columnas podemos usar _dot notation_ y tambien _col()___

__IMPORTANTE__ _sort_ se usa sobre columnas numericas, sobre _string_ no funciona bien.

### Spark ejercicio rapido

```
Para este ejercicio trabajamos con el archivo OfficeData.csv
```

Se pide.
1. Crear un DF ordernado por la columna __bonus__ de forma ascendente
2. Crear un DF ordenado por las columnas __age__ y __salary__ en orden descendente y ascendente.
3. Crear un DF ordenado por __age__ __bonus__ y __salario__ en orden descendente, descentente y ascendente.

__Ver solucion en Notebook: 13. Spark ejercicio rapido - Sort() u orderBy()__


### Spark groupBy

```
Se usa para agrupar sobre una o varias columnas.
```

1. __GroupBy en una columna.__

```python
df.groupBy("gender").show()
```

Esto así como está no se peude hacer, luego de hacer agregacion se debe hacer alguna __agregacion__

```python
df.groupBy("gender").count().show()
df.groupBy("gender").sum("marks").show()
df.groupBy("gender").avg("marks").show()
```

2. __GroupBy en mas de una columna.__

```python
from pyspark.sql.functions import col

df.groupBy(col("gender"), col("age")).count().show()
```

3. Como hacer agegaciones multiples.

Cuando hacermos __.groupBy()__ sobre un DataFrame no podemos concatenar las agregaciones, por ejemplo __.groupBy().count().sum().show()__ Esto no está permitido.

Para poder agregar mas de una agregacion debemos importar las funcions __from pyspark.sql.functions import sum, avg, max, min, mean, count__ y al momento de referenciar una agregacion lo hacemos dentro de __.agg()__

```python
from pyspark.sql.functions import sum, avg, max, min, mean, count

df.groupBy(col("gender"), col("age"))\
    .agg(count("*"),\
        min("marks"), \
            sum(col("marks"))).show()
```

4. Cambiar nombre de las columnas de  funciones de agregacion

__Podemos cambiar el nombre de las columnas de agregación usando la funcion _.alias()___

```python
from pyspark.sql.functions import sum, avg, max, min, mean, count

df.groupBy(col("gender"), col("age"))\
    .agg(count("*").alias("cantidad_total")\
        ,min("marks").alias("nota_minima")\
            , sum(col("marks")).alias("suma_notas")).show()
```

5. __Cómo funciona _groupBy()___

### Spark groupBy & Filtering

```
Si hacemos un filter o .filter() antes de un .groupBy() el agupado se hace solo el DataFrame resultante despues del filtrado.
```
__En este ejemplo el agrupado es solo sobre los registros del filtro por edad > 35__

```python
df.filter(col("marks") > 35).groupBy(col("gender"),col("age")).agg(count("*").alias("total_gender")).show()
```

Si ahora quiero filtrar solo los __total_gender__ > 100 concatenando un __.filter()__ no voy a poder porque me indica que esa columna no existe, esto se debe a que aun sigo trabajando sobre el __rdd df__ 
Para poder aplicar este filtro debo generar un __rdd__ con la primer consulta y aplicar al nuevo __rdd__

```python 
rdd_df.filter(rdd_df.total_gender > 50).show()
```

Pero podemos solucionarlo con un artilugio usando __col()__

```python
df.filter(col("marks") > 35).groupBy(col("gender"),col("age")).agg(count("*").alias("total_gender")).filter(col("total_gender") > 50).show()
```

### Spark ejercicio rapido

```
Para este ejecucio vamos a utilizar el DataSet StudentsData.csv
```

Se espera.

1. El numero total de estudiantes en cada curso.
2. EL numero total de H y M en cada curso.
3. EL total de notas por cada genero en cada curso.
4. Por cada curso, el maximo, minimo y AVG por grupo de edad.


__Ver resolucion en Notebook__