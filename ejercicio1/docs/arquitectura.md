# ETL Engineer Challenge - Airflow + MinIO + Polars + Trino

## Objetivo

Este proyecto implementa un proceso ETL local usando Apache Airflow, MinIO, Polars y Trino. El flujo ingesta un archivo CSV con transacciones historicas, lo almacena en una capa landing, lo procesa con Polars, genera una salida agregada en formato Parquet y la publica como tabla externa consultable desde Trino.

El proceso tambien genera una salida de calidad con registros observados, permitiendo detectar inconsistencias sin aplicar correcciones de negocio no validadas.

## Arquitectura general

La solucion se ejecuta sobre Docker Compose y utiliza los siguientes componentes:

- Apache Airflow como orquestador.
- MinIO como Data Lake compatible con S3.
- Polars como motor de procesamiento.
- boto3 como cliente S3 compatible para leer objetos desde MinIO.
- Trino como motor SQL.
- Hive Metastore como catalogo de metadata.
- MySQL como base del metastore.
- PostgreSQL como base de metadata de Airflow.
- Redis como broker para CeleryExecutor.
- DBeaver como cliente externo para validacion SQL.

## Patron de diseño

Se implemento un patron de fabrica de DAGs. El archivo ``etl_factory.py`` no contiene logica especifica de negocio; su responsabilidad es leer archivos ``pipeline.json``, validar la configuracion, cargar dinamicamente el modulo de procesamiento y construir las tareas de Airflow de acuerdo con el campo ``order``.

Este enfoque permite agregar nuevos pipelines sin duplicar codigo de orquestacion.

### Patron de diseño: DAG Factory
Se eligio el ``patron de fabrica`` para evitar definir DAGs manualmente en codigo Python por cada proceso. En su lugar, el archivo ``etl_factory.py`` actua como una fabrica generica que lee archivos ``pipeline.json``, valida su estructura, carga dinamicamente la funcion de procesamiento correspondiente y construye las tareas del DAG en el orden definido por configuracion.

#### Ventaja
1. Permite crear nuevos DAGs agregando configuracion JSON.
2. Reduce duplicacion de codigo.
3. Separa orquestacion de logica de negocio.
4. Permite reutilizar modulos comunes como MinIO, Trino y validaciones.
5. Facilita mantener multiples pipelines con una estructura estandar.

#### Clave
El factory no conoce la logica interna del ETL.
Solo sabe:

- leer configuracion
- cargar un callable dinamico
- crear tasks
- conectar tasks por order


## Datalake: Landing y Bronze

Se utilizo MinIo como servicio de almacenamiento, compatible con S3 usado como Data Lake local. Se crean dos buckets:

- ``bck-landing``: entrada original sin procesar.
- ``bck-bronze``: salida procesada y archivos de calidad.

``Landing``:
Contiene el archivo original como fue recibido. No se modifica ni se corrige. Sirve como evidencia y punto de reproceso.

``Bronze``:
Contiene la informacion procesada en formato Parquet. En este proyecto se generan dos zonas:

- master: salida agregada solicitada por el ejercicio.
- quality: registros observados con inconsistencias detectadas.

### Estructura 
```
bck-landing/
└── data/
    └── data_prueba_tecnica.csv

bck-bronze/
├── master/
│   └── data_prueba_tecnica.parquet
└── quality/
    └── observed_records.parquet
```