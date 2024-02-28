# Tutorial PySpark & AWS

1. [Introducción](#1.-introduccion)
    - [Materiales del curso](#.-materiales-del-curso)
2. [About Spark](#2.-about-spark)
    - [Ecosistema de Spark](#.-ecosistema-de-spark)
    - [Crear una cuenta con Databricks](#.-crear-una-cuenta-con-databricks)
3.  [Spark RDD](#3.-spark-rdd)
    - [Creacion de un RDD](#.-creacion-de-un-rdd)
    - [Submit un job en el cluster](#.-Submit-un-job-en-el-cluster)
    - [Spark RDD functions](#.-spark-rdd-functions)
    - [Ejercicio rapido 1](#.-ejercicio-rapido-1)
    - [Spark flatMap](#.-spark-flatMap)
    - [Spark rdd filter](#.-spark-rdd-filter)
    - [Ejercicio rapido 2](#.-ejercicio-rapido-2)



## 1. Introduccion

PySpark es un Wrapper de Spark para Python. Se usa principalmente para el procesamiento en __streaming__ o __batch__ processing.

Tambien se puede usar en otros proyectos como __Machine Learning__

Unos de los principales casos de uso de __Spark__ es la creación de __pipelines__ ya que puede interecatuar con varios origenes y destinos de datos y __Full load__ y __Replication on going__ que es la carga masiva de datos y la posterior carga de lo que cambió en la fuente de los mismos.

```
El proyecto final será de CDC con Spark, Hadoop y AWS.
```

## Materiales del curso

[Link a GITHUB](https://github.com/AISCIENCES/course-master-big-data-with-pyspark-and-aws/tree/main)

## 2. About Spark

Spark es _rápido_ a comparacion de otros softwares que hacen lo mismo, es __distribuido__ permite tener o acceder archivos particionados en varios clusters y paralelizar.
Permite hacer analisis en tiempo real o __Streaming Processing__ y proveé una caché para los datos que son accedidos de forma repetida. Tambien es __Fault Tolerant__ si un nodo o worker se caé el trabajo es redirigido a otros.

## Ecosistema de Spark

EL cosistema de Hadoop esta formado por:

|artefacto|Descripcion|
|---------|-----------|
|HDFS|Es el file system de hadoop - distribuido (Storage)|
|YARN| Es el equivalente a un sistema Operativo|
|MapReduce| Es la tecnica para mapear datos |
|Spark| Resuelve las dificultades de MapReduce ya que Spark es una abstracción de Hadoop|


## 1. Arquitectura de Spark

Lo primero es el __Spark Context__ que contiene nuestro codigo a Ejecutar.
Luego está es __Cluster Manager__ que es el que controla y distribuye el trabajo entre los __Workers__
Finalmente estpan los __Workers__ que son los que se encargan de hacer el procesamiento necesario. 

## 2. Spark Ecosistem

El ecosistema de Spark incluye __SPARK SQL__ , __STREAMING SPARK__ , __SPARK MLlib__ y __SPARK GRAPHX__
Tambien tiene una API que permite trabajar con Java, Python y Scala.

## Crear una cuenta con Databricks

[Link DataBricks](https://community.cloud.databricks.com/login.html)

1. Crear Cuenta.

![Databricks account](./img/databricks-create-account.png)

2. Elegimos __COMUNITY EDITION__ FOR PERSONAL USE.

3. Dentro de Databricks debemos crear un __cluster__ o en la nueva version __create compute__

![create-cluster](./img/databricks-create-cluster.png)

4. Creamos una __Notebook__ y la ejecutams sobre __Python__ y seleccionamos el cluster que creamos.

![databricks-notebook](./img/databricks-notebook.png)

Vemos que en la Notebook tenemos: EL interprete de Python que estamos usando y el __cluster__ al que estamos conectados.


## 3. Spark RDD

```
Son el componente central y mas importante de Spark. 
RDD son las siglas de Resiliant Distributed Dataset, tienen como propiedad que son inmutables y una coleccion de objetos distribuida.
Estos rdd estas distribuidos en los distintos clusters.
```

__IMPORTANTE__ RDD fué la implementación de Spark. Hoy no est tan utilizada pero si se necesita bajar el nivel de Abstracción son muy utiles.
EN la actualidad los __RDD__ fueron reemplazados por __DataFrames__ y estos por __SPARK SQL__

### Transformaciones y Acciones.

Una transformacion es siempre Lazy y crea un nuevo RDD a paritr de uno existente.
Una acción son las que disparan el flujo de datos. 

__Lazy Evaluation__ significa que no procesa hasta que es requerido por medio de una accion.

### Creacion de un RDD

En __SparkConfig__ es donde especificamos los origenes de datos o la configuracion necesaria para poder leer datos. 

__SparkContext__ es la parte central, es el entrypoint del programa. 

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

text = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/sampletext.txt")

miFile  = text.collect()
```

En este ejemplo __spark.textfile__ es una transformacion __lazy__ mientras que __collect()__ es una accion sobre el RDD que ejecuta la __lectura del archivo__

__Que pasa con el tipo de dato en cada una de estas variables?__

Cuando aplicamos la transformacion __spark.textfile__ crea un __rdd__ en la variable text, pero cuando sobre esta variable aplicamos __text.collect()__ esta accion devuelve una __lista__ haciendo que ya no se pueda usar __text__ para aplicar nuevas transformaciones, siempre se debe trabajar con __rdd__ y lo pultimo debe ser una __accion__


### Submit un job en el Cluster

El job anterior __ejemplo_submit.py__ le podemos hacer un submit al cluster.

```shell
spark-submit ejemplo_submit.py
```

### Spark RDD functions

1. __RDD MAP__ (Lambda)

Se usa para hacer un mapeo de datos de un estado a otro.
Como resultado crea otro RDD

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

text = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/sampletext.txt")

miFile  = text.map(lambda x:x.split(' '))

```

En este ejemplo la transformacion __map__ se aplica sobre el __rdd__ text y devuelve otro __rdd__ miFile con los registros separados por " ".

2. __Otras funciones de RDD__

A veces no queremos usar funciones __lambda__ asique podemos usar __funciones definidas por el usuario__

En este ejemplo lo que queremos hacer son dos cosas.
1. Separar la cadena de strings en 4 arrays
2. Castear cada elemento dentro del array a __int__ y sumarle 2.

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

text = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/sampletext.txt")

def mi_foo(x):
    l =  x.split(' ')
    ls = []
    for num in l:
        ls.append(int(num) + 2)
    return ls

miFile  = text.map(foo)
miFile.collect()
```

3. Ejercicio rapido


__sin lambda function__

Tenemos un archivo de texto con 3 frases. Queremos generar un array por cada frase con la logitud de cada palabra. 

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

fileText = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/wordcount.txt")

def foo(x):
    array = x.split(' ')
    l2 = []
    for word in array:
        l2.append(len(word))
    return l2

fileText.map(foo).collect()
```
__con lambda function__

hacemos el mismo ejercicio pero usando __lambda__.
Primero vamos a generar la lista de listas usando una __lambda__ con __split(' ')__ y a este resultado le agregamos una __list comprehention__ para determinar el numero de letras de cada palabra.

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

fileText = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/wordcount.txt")

fileText.map(lambda x:[len(word) for word in x.split(' ')]).collect()
```

### Spark flatMap

Devuelve los elementos como una unica entidad del mismo tipo.
Si tenemos una lista de lista, devuelve todo como un tipo de dato __unificado__ una sola lista generando un nuevo __rdd__

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

fileText = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/wordcount.txt")

fileText.flatmap(lambda x:x)
```

### Spark rdd filter

Se usa para elminar elementos de un __RDD__ tambien genera un nuevo __rdd__
Si la expresión final de la funcion lambda es true será considerada en el retorno de la función, de otra forma no.

```python
from pyspark import SparkContext, SparkConfig

cf = SparkConfig().setAppName('readFile')
spark = SparkContext.getOrCreate(conf=cf)

fileText = spark.textfile("/mnt/d/Proyectos/Tutorial-SparkAWS/data/wordcount.txt")

fileText.filter(lambda x: x in ('10 23 45 67', '87 54 34 101')).collect()
```

Esta expresión es de prueba porque siempre será __True__

__Podemos usar filter sin necesidad de una lambda function__ creando nuestra propia __UDF__

```python 
def foo(x):
    if x in ('10 23 45 67', '87 54 34 101'):
        return True
    return False

fileText.filter(foo).collect()
```

### Ejercicio rapido 2