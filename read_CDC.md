# Change Data Capture with PySpark

1. [Introduction](#1.-introduction)
2. [Proyecto y Arquitectura](#2.-proyecto-y-arquitectura)
3. [Creacion RDS](#3.-creacion-de-rds)
4. [Creacion S3 Bucket](#4.-creacion-s3-bucket)
5. [Creacion de DMS](#5.-creacion-de-dms)
    - [DMS - Source EndPoint]()
    - [DMS - Destination EndPoint]()
    - [DMS Instancia]()
6. [RDS y Dumping Data Connection](#6.-rds-y-dumping-data-connection)
7. [DMS full load](#7.-dms-full-load)
8. [DMS replication ongoing](#8.-dms-replication-ongoing)
9. [Stop instances](#9.-stop-instances)
10. [Glue jobs](#10.glue-jobs)
    - [Full Load]()
    - [Change capture]()
    - [Glue Job CDC]()
11. [Lambda function y Trigger](#11.-lambda-function-y-trigger)
    - [Testing y trigger]()
    - [Obtener FileName]()
    - [Creacion de Glue Job]()
    - [Testing Invoke]()
    - [Writing Glue Shell Job]()
12. [Full load pipeline](#12.-full-load-pipeline)
13. [CDC pipeline](#13.-cdc-pipeline)


## 1. Introduction

CDC sirve cuando tenemos una __base de datos__ que queremos replicar o capturar los cambios dentro de la misma y guardarlo en un archivo o __DataLake__

Los cambios que podemos capturar son __Delete__, __Insert__, __Update__

## 2. Proyecto y Arquitectura

Buscamos que todos los cambios que ocurran dentro de la __Base RDB__ se vean replicados en __HDFS / S3__

Para capturar estos datos vamos a crear un servicio en AWS llamado __DMS__ o __Data migration Service__ que deje en un __Bucket temporal__ el archivo con los cambios.

Cuando llegue un nuevo archivo al __Bucket Temporal__ esto dispara un trigger que activa una __lambda__ que llamará a un __job de PySpark__ usando GLUE pasandole el FileName.

Este __Job de GLUE__ va a leer data del __Bucket Temporal__ y lo pondrá en el __bucket FINAL: HDFS/S3__

![](./img/cdc-01.png)

## 3. Creacion de RDS

Lo primero que hacemos es crear la Base de datos __RDS__ con MySql, __USAR FREE TIER__

Nombre de la instancia: __mysqlinstance__

Para este proyecto usamos MySql y nos seguramos de usar la capa libre.

![](./img/cdc_mysql_01.png)

Elegimos un nombre de la Instancia y el nombre de usuario es __admin__

![](./img/cdc_mysql_02.png)

Nos aseguramos de estar usando __t3.micro__ porque es gratuito.

![](./img/cdc_mysql_03.png)

En conectividad dejamos los valores __default__

![](./img/cdc_mysql_04.png)

Lo mas importante viene en __configuración adicional__. Acá debemos activar el __backUp__ de la BD ya que es necesario para poder trabajar con __DMS__ y debemos crear __un grupo de parametros adicional__

![](./img/cdc_mysql_05.png)

__Creacion de un grupo de parametros adicional__

Abrimos otra ventana de __RDS__ y seleccionamos __Param Groups__
Creamos el nuevo Grupo para MySql 8.0. Las versiones deben coincidir. 

![](./img/cdc_mysql_06.png)

Visualizamos el Grupo creado.

![](./img/cdc_mysql_06-1.png)

Al crear el grupo, debemos cambiar el valor de un parametro __binlog_format__ a __ROW__ y guardarlo. 

![](./img/cdc_mysql_06-2.png)

Con el nuevo grupo disponible lo que hacemos es seleccionar el __Grupo de parametros__ en la ventana de creacion de la BD. Si no se actualiza solo, es necesario hacer __F5__ y volver a cargar.

![](./img/cdc_mysql_07.png)

Y creamos la __BD__

![](./img/cdc_mysql_08.png)

__IMPORTANTE__ REcordar que para poder logarnos en la instancia de BD debemos tener las reglas de __Inbound__ de la VPN activas para 0.0.0.0/0 Todas.

![](./img/cdc_mysql_09.png)

Si no tiene las reglas correctas las creamos.

Paso 1.

![](./img/cdc_mysql_10.png)

Paso 2.

![](./img/cdc_mysql_11.png)


## 4. Creacion de S3 Bucket

La segunda parte y __antes de crear los EndPoints__ vamos a crear el __S3 Bucket__.

Creamos un bucket con un nomber unico. __mysql-cdc-bucket__

![](./img/cdc-bucket-01.png)

Hacemos que el bucket sea publico

![](./img/cdc-bucket-02.png)

Y finalmente creamos el bucket

![](./img/cdc-bucket-03.png)


## 5. Creacion de DMS

En el tercer punto vamos a crear el __DMS__ o __Data Migration Service__

[Qué es Data Migration Service?](https://youtu.be/ouia1Sc5QGo)

[Caso de uso](https://docs.aws.amazon.com/dms/latest/oracle-to-aurora-postgresql-migration-playbook/index.html)


### 5.1 DMS - Source EndPoint

Creamos el Source Endopint y nos aseguramos de elegir la BD que creamos para la replicacion.

![](./img/cdc-endpoint-01.png)

Automaticamente se cargan los datos de la __DRS__ y especificamos el usuario y password creados.

![](./img/cdc-endpoint-02.png)

Creamos el __ENDPOINT___

![](./img/cdc-endpoint-03.png)

### 5.2 DMS - Destination EndPoint

Creamos el destination Endpoint para el S3 Bucket.

Para poder crear un __endpoint__ para S3 debemos crear un Role IAM.

Primero creamos un __endpoint__ destino

![](./img/cdc-bucket-endpoint-01.png)

Antes de seguir debemos crear un Role IAM que tenga permisos sobre DMS 
__Esto lo hacemos desde otra ventana al servicio IAM__

![](./img/cdc-bucket-endpoint-02.png)

y FullAccess al Bucket S3.

![](./img/cdc-bucket-endpoint-03.png)

vemos los detalles del role y lo creamos.

![](./img/cdc-bucket-endpoint-04.png)

Lo que nos importa del role es su ruta, la cual copiamos y luego pegamos para crear el endpoint.

![](./img/cdc-bucket-endpoint-05.png)

Cargamos los datos faltantes en el endpoint con el nuevo role creado.

![](./img/cdc-bucket-endpoint-06.png)

Terminamos creando el EndPoint a un destino.

![](./img/cdc-bucket-endpoint-07.png)

### 5.3 DMS Instancia

Hasta ahora tenemos La base de datos, el Bucket S3 y los dos EndPoints.
Ahora necesitamos crear la __INSTANCIA DE REPLICACION__ que usara DMS.

Dentro de DMS Service.

![](./img/cdc-instancia-rep-01.png)

Lo mas importante al crear una instancia es hacerlo usando __t.micro__ gratuito y acceso publico.

![](./img/cdc-instancia-rep-02.png)

## 6. RDS y Dumping Data Connection

Una vez creada la primera parte de la Arquitectura vamos a probar insertar datos.

Para esto usamos el archivo __DUMP__ en el directorio ./CBC_DATA.
EL mismo tiene la creacion del esquema, la tabla y los Inserts.

__ES MUY IMPORTANTE QUE LA TABLA TENGA PK, SINO REPLICATION ONGOING NO FUNCIONA__

Primero vamos a ejecutar la creacion del schema, la tabla y los inserts.

![](./img/cbc-dump-data-01.png)

## 7. DMS Full Load

Ahora, despues de haber insertado los primeros datos podemos iniciar un proceso de migracion de datos con lo que tenemos hasta ahora.

1. ingresamos al servicio AWS DMS y al S3 para ver que los datos se estén copiando.

Dentro de DMS, creamos una tarea de __migracion de datos__

Elegimos un nombre para la Tarea, la instancia de replica que creamos y los EndPoints. En este punto debemos elegir __el tipo de replica__, en este caso elegimos __full load__ para que tome todos los cambios mas lo que ya existe. 

![](./img/cdc-full-load-01.png)

2. En las propiedades de la tabla dejamos todo tal cual pero cambiamos la cantidad de tablas a migrar en paralelo. Por defecto son 8 pero nosotors solo tenemos una.

![](./img/cdc-full-load-02.png)

3. En asignación de tablas especificamos los datos del esquema y la tabla que debe mirar el proceso.
Podemos usar __wildcards__ para elegir mas de una tabla o esquema.

![](./img/cdc-full-load-03.png)

4. DEsactivamos la evaluacion previa y especificamos que la nueva tarea se ejcute por primera vez al momento de ser creada.

![](./img/cdc-full-load-04.png)

5. Vemos que la tarea se creó y luego se ejecutó.

![](./img/cdc-full-load-05.png)

6. Si vamos a S3 vemos que se creó un directorio con la estructura __esquema/tabla/datos.csv__

![](./img/cdc-full-load-06.png)


## 8. DMS replication Ongoing

Luego de haber creado el __full load__ ahora podemos empezar a hacer cambios en la BD y ver como toma los cambios.

```sql
Select * from ahmad_schema.Persons
update ahmad_schema.Persons set FullName = 'ABC XYZ' where PersonId = 7;
INSERT INTO ahmad_schema.Persons VALUES (130,'Alica Bing','New York');
INSERT INTO ahmad_schema.Persons VALUES (131,'Malinda Bing','Detroit');
INSERT INTO ahmad_schema.Persons VALUES (132,'Chandler Bing','Portland');
update ahmad_schema.Persons set FullName = 'ABC XYZ' where PersonId = 8;
DELETE FROM ahmad_schema.Persons where PersonId = 10;
```

Ejecutamos este SQL y vamos al __BUCKET__ a ver el nuevo archivo generado.

![](./img/cdc-repli-ongoin-01.png)

Vemos lo que hay dentro del nuevo archivo generado.

```
U,7,ABC XYZ,Phoenix
I,130,Alica Bing,New York
I,131,Malinda Bing,Detroit
I,132,Chandler Bing,Portland
U,8,ABC XYZ,Denver
D,10,Jack Hicks,Houston
```

|accion|dato anterior|dato nuevo|
|------|-------------|----------|
|u-update|Phoenix|ABC XYZ|
|i-insert|no tiene|toda la linea
|d-delete|todo lo que trae|no tiene|


## 9. Stop Instances

Hasta ahora tenemos desarrollada la primera parte de la Arquitectura.

![](./img/cdc-01-02.png)

Como vamos a seguir trabajando en las funciones Lambda es una buena práctica detener la instancia para que no consuma recursos y __$$$__

1. Primero detenemos la tarea de migracion.

![](./img/cdc-stop-instance-01.png)

2. Luego detemos la instancia de DB.

![](./img/cdc-stop-instance-02.png)

## 10. Glue Jobs

Antes de crear la funciones lambda vamos a crear el __job de GLUE__.

Son los scripts. EL primero se encargará del __full-load__ y el segundo del __onGoing__


### 10.1 Full Load

1. Se crea el .py con nombre __SparkCDCFullLoad.ipynb__

Este archivo script tomará del __S3__ temporal el file full load y lo moverá al __S3__ final.

### 10.2 Change Capture

1. Este segundo proceso o script será el que captura los cambios. Tiene la escencia del proceso __CDC__

Por ahora lo vamos a poner dentro del mismo archivo ___SparkCDCFullLoad.ipynb__

__Pasos 3 y 3.1__

### 10.3 Glue Job CDC

Con el archivo creado (Script) agregamos la logica.
Para esto vamos a iterar sobre los registros del file __replication onGoing__ con tres opciones: una para Insert, Delete y otra para Update.

__Paso 4.__

En esta parte usamos el archivo de __Replication onGoing__ y le hacemos un loop por cada tipo de __operacion__ a cada registro.

Los que vamos a ahcer es replicar las operaciones que se hicieron en la __BD__ pero sobre el archivo __FullLoad__

1. Delete

Para elminar un registro del archivo __fullLoad__ lo que hacemos es volver a crear el archivo con un Filtro para no mostrar la __row__ que esta en el archivo __Replication onGoing__ 

```python
dfFullLoad = dfFullLoad.filter(col("id") != row["id"])
```

2. Update

Update es un poco mas difil, paa esto usamos el siguiente código.

```python
fdFullLoad =fdfFullLoad.withColum("fullName", \
    when(row["id"] == col("id"), row["fullName"]).\
        otherwise(fdFullLoad["fullName"]))

fdFullLoad = fdFullLoad.withColun("city", \
    when(row["id"]==col("id"), row["city"]).\
        otherwise(fdFullLoad["fullName"]))
```

3. Insert

Para hacer insert debemos usar la __row__ que se general en el for loop y convertirla a __DF__ y hacerle un __union__ al FD final.

```python
insertedRow = [list(row[1:])]
columns = ("id", "fullName", "city")
df_row = spark.createDataFrame(insertedRow, columns)
fdFullLoaf.union(df_row)
```

El union se hace por nombre de columna.


## 11. Lambda function y Trigger

EN esta seccion vamos a crear una funcion __lambda__ que recibe el nombre de un archivo y se lo pasa al script creado __en el punto anterior__
Esta funcion lambda se dispara cuando llega un nuevo archivo al __bucket temporal__.

CReamos una nueva funcion lambda desde __el servicio AWS Lambda__

![](./img/cdc-lambda-01.png)

El servicio lambda debe tener __un roll asociado para poder ejecutar__ asique creamos uno.

![](./img/cdc-lambda-02.png)

EL mismo desde tener tres perfiles o permisos full access. Para el Bucket, CloudWatch (logs) y Glue

![](./img/cdc-lambda-03.png)

Una vez creado volvemos a la pantalla de __lambda functions__ y seleccionamos el AIM Roll creado y creamos la funcion.

![](./img/cdc-lambda-04.png)

COn la funcion lambda creada vamos a __crear el trigger o desencadenadoe__ de S3.

![](./img/cdc-lambda-05.png)

Seleccionamos tipo desencadenador __S3__ y el bucket creado para el proyecto __CDC__ __Cuando creamos un desencadenador podemos elegir la carpeta que debe ver dentro del S3 o ponerle un prefeijo para que solo vea .csv__ en este caso lo dejamos por defecto. 

![](./img/cdc-lambda-06.png)

Con la funcion lambda y su desencadenador creada podemos testearla.

![](./img/cdc-lambda-07.png)

### 11.1 Testing Trigger

Para probar la funcion lambda primero debemos hacer un deploy de la misma, crear una prueba y ver el log usando __cloudWatch__

Hacemos __deploy__ de la funcion lambda.

![](./img/cdc-lambda-08.png)

Creamos una prueba de test con el nombre __abc__

![](./img/cdc-lambda-09.png)

Corremos una prueba solo para ver que la funcion compile.

![](./img/cdc-lambda-10.png)

Para ver los logs que se generan en las corridas de la funcion lambda vamos al __servicio CloudWatch__ y buscamos los grupos de eventos asociados a la funcion lambda. Buscamos el ultimo __log__

![](./img/cdc-lambda-11.png)

En el registro vemos la primer prueba que se ejecutó de forma manual.

![](./img/cdc-lambda-12.png)

Para probar la __funcion lambda__ vamos a cargar un archivo en el __bucket S3__ para ver la funcion se dispare.

![](./img/cdc-lambda-13.png)

Cuando volvemos al __servicio CloudWatch__ vemos que la funcion efectivamente ejecutó.

![](./img/cdc-lambda-14.png)


### 11.2 Obtener FileName

Para continuar con el desarrollo vamos a intentar obetener el nombre del archivo que dispara a la funcion lambda.

```python
import json

def lambda_handler(event, context):
    print("Hello world from Py")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
```

Este es el codigo de nuestra funcion lambda, donde los parametros de entrada __event__ contienen la informacion de lo que disparó la ejecución.

__event__ es un json al que podemos acceder para obetener los datos del __bucket__ y su contenido.

```python
bucketName = event["Records"][0]["s3"]["bucket"]["name"]
fileName = event["Records"][0]["s3"]["object"]["key"]
```

__IMPORTANTE__ una vez que hacemos cambios en la funcion recordar hacer __deploy__

ejemplo de JSON de respuesta de un evento

```YML
{'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2024-04-06T05:09:32.123Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'A2X7NO3O3LNA9Y'}, 'requestParameters': {'sourceIPAddress': '152.168.40.136'}, 'responseElements': {'x-amz-request-id': '9ENCT4964NBT1BDT', 'x-amz-id-2': 'lg5X4A2fAqDgr2+sFN3xAYQW3mZ4rNm9Pcn5/Yr4cN1eJRVF0Af4JCt5LQKdVkyIipVMMHtITKUFTXhJqfXLrzgPKJpMSc4Q'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'e2905f2c-9e54-4d15-8d81-2f069a122434', 'bucket': {'name': 'mysql-cdc-bucket', 'ownerIdentity': {'principalId': 'A2X7NO3O3LNA9Y'}, 'arn': 'arn:aws:s3:::mysql-cdc-bucket'}, 'object': {'key': 'cdc-lambda-15.png', 'size': 49963, 'eTag': 'af6671c29a155a0536dec5c963f10f90', 'sequencer': '006610D90C105CBB79'}}}]}
```

Le agregamos a nuestra __lambda__ las variables para capturar el number del bucket y del archivo cargado. REcordar hacer un nuevo __Deploy__ para que tome los cambios.

![](./img/cdc-lambda-15.png)

Cargamos el nuevo archivo en el __S3__ y buscamos el __log en CloudWatch__. Seguramente no usa el mismo Treath que antes, puede haber creado una nuevo.

![](./img/cdc-lambda-16.png)

Vemos el contenido de la variable __event__ en el Log.

![](./img/cdc-lambda-17.png)

Ya tenemos como capturar el nombre del Bucket y El archivo, ahora necesitamos crear el Job de Glue y pasar estos datos.


### 11.3 Creacion de Glue Job

Antes de poder crear un job de glue debemos tener un roll asociado al mismo, el cual debe tener permisos __fullAccess__ sobre S3 y CloudWatch.

![](./img/cdc-glue-01.png)

LE damos un nombre al roll y vemos los permisos finales.

![](./img/cdc-glue-02.png)

Ahora vamos a crear un Job de Glue desde la consola de GLUe, elegimos la Opcion __Script__ -> job Spark y en la pestaña __Job details__ ponemos:

![](./img/cdc-glue-03.png)

Lo mas importante en esta pantalla es especificar el Roll de Glue y poner la cantidad de Procesadores en 2 para no consumir muchos recursos.

![](./img/cdc-glue-04.png)

### 11.3.1 Agregamos el Job de Glue a la Funcion Lambda.

Cuando creamos la funcion lambda vemos que la pantalla principal nos da dos opciones, Source trigger y destination.
En Source trigger seleccionamos __S3__ pero los destinations de Lambda no incluyen __Job Glue__ por lo tanto debemos agregarlo en el codigo.

![](./img/cdc-glue-05.png)

para poder agregar servicios de AWS como codigo usamos __boto3__

```python
import json
import boto3



def lambda_handler(event, context):
    print("Hello world from Py")
    print(event)
    
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]
    
    glue = boto3.client('glue')

    response = glue.start_job_run(
        JobName = 'glueCDC-pysark',
        Arguments = {
            '--s3_target_path_key': fileName,
            '--s3_target_path_bucket': bucketName
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


```

Cuando modificamos la funcion recordad hacer __Deploy__

![](./img/cdc-glue-06.png)

Dentro del Job de Glue que creamos anteriormente lo vamos a __Editar__ y colocamos el siguiente codigo que hace referencia a las variables que se pasan desde __lambda__

```python
import sys

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['s3_target_path_key', 's3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)
```

![](./img/cdc-glue-07.png)

Puede ser que dependiendo de la version de GLUE veamos mas lineas de codigo.

__IMPORTANTE__ No usar comillas dobles para referencias variables.


### 11.4 Testing Invoke

Habiendo creado el Job de Glue y referenciado las variables, ahora vamos a cargar un archivo valido en __S3__

![](./img/cdc-glue-08.png)

Vemos en el log de Glue que muestra las dos variables que se debian pasar de la lambda al job de Glue.

![](./img/cdc-glue-09.png)


### 11.5 Writing Glue Shell Job

Despues de haber probado el Job escribimos el job restante usando Spark.

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

args = getResolvedOptions(sys.argv, ['s3_target_path_key', 's3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-output-pyspark-nleali/output"

spark = SparkSession.builder.appName("CDC").getOrCreate()

if "LOAD" in inputFilePath:
    fldf = spark.read.csv(inputFilePath)
    fldf = fldf.withColumnRenamed("_c0", "id").\
        withColumnRenamed("_c1", "fullName").\
            withColumnRenamed("_c2", "City")
    
    fldf.write.mode("overwrite").\
        csv(finalFilePath)
else:        
    
    print("Entramos por ELSE")
    udf = spark.read.csv(inputFilePath)
    udf = udf.withColumnRenamed("_c0", "action").\
        withColumnRenamed("_c1", "id").\
            withColumnRenamed("_c2", "fullName").\
                withColumnRenamed("_c3", "city")
                
    print("Cargo Onchaing")
                
    ffdf = spark.read.csv(finalFilePath)
    ffdf = ffdf.withColumnRenamed("_c0", "id").\
            withColumnRenamed("_c1", "fullName").\
                withColumnRenamed("_c2", "City")
                
    print("Cargo Full Load")

    for row in udf.collect():
        print("{} - {}".format(row, type(row)))
        if row['action'] == 'U':
            print('Update')
            ffdf = ffdf.withColumn("fullName", when(row["id"] == col("id") ,\
                row["fullName"]).otherwise(ffdf["fullName"]))
            ffdf = ffdf.withColumn("city", when(row["id"] == ffdf["id"], \
                row["city"]).otherwise(ffdf["city"]))
        
        
        elif row['action'] == 'D':
            print('Delete')
            ffdf = ffdf.filter(col("id") != row["id"])
            
        else:
            print('Insert')
            insertedRow = [list(row[1:])]
            columns = ("id", "fullName", "city")
            newdf = spark.createDataFrame(insertedRow, columns)
            ffdf = ffdf.union(newdf)
      
    print("grabamos el ardhivo final")    
    ffdf.write.mode("overwrite").csv(finalFilePath)
```

## 12. Full load pipeline

Para terminar con el __pipeline__ debemos reiniciar los servicios que habiamos desactivado.

__IMPORTANTE__ Antes de reactivar todos los servicios, recordar vaciar compleamente los buckets.

Para esto primero inciamos la __RDS: Mysql__ y luego el servicio __DMS__.

1. Reiniciamos la BD.

![](./img/cdc-pipeline-01.png)

2. Una vez reiniciado la BD, inciamos el servicio de Migracion de Datos.

![](./img/cdc-pipeline-02.png)

Con el servicio inciado se va a disparar todo el trigger, pero esta vez no va a usar los datos originales sino que puede ser que si ya hicimos algun update solo se migren en el LOAD (Carga inicial) lo que ya está.

![](./img/cdc-pipeline-03.png)


4. Vemos que inició el pipeline y creo el archivo LOAD en el bucket temporal y este fué tomado por la __Lambda__ que llamó al proceso __glue__ y lo migró al __bucket final__

![](./img/cdc-pipeline-04.png)

5. Miramos los logs de cloudWatch - Lambda

En el Full Load vemos que toma el archivo LOAD y lo pasa al GLUE

![](./img/cdc-pipeline-05.png)

6. Miramos los logs de cloudWatch - Glue

Vemos que en /outputs/ recibió el archivo y lo copió al __Bucket final__ por ser Load. Y no tiene otro output.

![](./img/cdc-pipeline-06.png)

## 13. CDC Pipeline

Una vez ejecutado el __job Glue__ que copia el __FullLoad__ ahora vamos a generar unos cambinos en la base de datos para ver como funciona el __Pipeline CDC__

1. Ejecutamos un Update

```sql
update ahmad_schema.Persons set FullName = 'Nicolas Leali' where personID = 15;
```

2. Vemos que al generar un cambio se creo un nuevo archivo en el __bucket temporal__

![](./img/cdc-pipeline-07.png)

3. La función Lambda leyó el archivo y lo pasó al proceso Glue.


![](./img/cdc-pipeline-08.png)


4. En el proceso Glue vemos que el archivo fué tomado y procesado.

![](./img/cdc-pipeline-09.png)


## 14. Eliminamos todos los servicios creados.

1. Primero eliminamos la base __RDS__. No vamos a guardar ninguna instantanea asique seleccionamos la opcion no guardar nada.

2. Elminamos la __Data Migration Task__. Primero hay que detenerla y luego eliminarla. Lego los endPoints y el servidor de migracion.

3. Elminamos la funcion lambda.

4. Elminamos los grupos de registro.

5. Eliminamos los buckets.

6. Elminamos el Job Etl Glue.

7. eliminamos los roles.



