## Ejercicio adicional - Pipeline ECOBICI

Para el ejercicio adicional se implemento un nuevo DAG utilizando el mismo stack del ejercicio principal:

- Apache Airflow como orquestador
- Polars como motor de procesamiento
- MinIO como Data Lake compatible con S3
- Trino como motor SQL para consulta
- DBeaver como cliente de validacion

El objetivo fue demostrar que el patron de fabrica de DAGs permite agregar nuevos pipelines sin modificar la logica base del factory, para este caso, se agrego un nuevo pipeline independiente llamado:

```text
ecobici_station_status_pipeline
```

## Dataset seleccionado

Se selecciono el dataset station_status de ECOBICI CDMX, publicado mediante el estandar GBFS

La URL base utilizada es:
```
https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json
```
Este archivo funciona como un indice de feeds, desde ahi el pipeline identifica el feed:
``station_status``
y obtiene dinamicamente la URL real del dataset:
```
https://gbfs.mex.lyftbikes.com/gbfs/es/station_status.json
```
Este dataset contiene el estado operativo y la disponibilidad actual de las estaciones de ECOBICI

Cada registro representa una estacion en un momento de actualizacion del feed e incluye informacion como:

- bicicletas disponibles;
- espacios disponibles para devolver bicicletas;
- si la estacion esta instalada;
- si permite rentar bicicletas;
- si permite devolver bicicletas;
- timestamp del ultimo reporte de la estacion.

En terminos de negocio, este dataset permite analizar la disponibilidad operativa de las estaciones a traves del tiempo

## Periodo de extraccion

El DAG se programo para ejecutarse cada 10 minutos:

```json
"schedule_interval": "*/10 * * * *",
```

Se selecciono este periodo porque el dataset ``station_status`` representa informacion dinamica de disponibilidad, as estaciones pueden cambiar rapidamente de estado debido a la renta y devolucion de bicicletas

Una frecuencia de 10 minutos permite capturar cambios relevantes sin generar demasiada carga en el ambiente local

Comparacion de criterios:

- Cada 5 minutos: mayor detalle, pero mas ejecuciones, mas escrituras y mayor crecimiento del historico.
- Cada 10 minutos: equilibrio entre observabilidad y costo de procesamiento.
- Cada 30 minutos: menor carga, pero puede perder eventos cortos como estaciones vacias, saturadas o temporalmente no disponibles.

Para este ambiente de desarrollo se eligio 10 minutos como punto medio razonable.



## Propuesta de limpieza de datos

El pipeline aplica una limpieza tecnica y validaciones basicas sobre los datos recibidos desde el feed ``station_status``.

Las principales reglas aplicadas son:

### Casteo de tipos
``station_id`` se convierte a string.
``num_bikes_available`` se convierte a entero.
``num_docks_available`` se convierte a entero.
``is_installed``, ``is_renting`` e ``is_returning`` se convierten a enteros.

### Conversion de timestamps
- ``last_reported`` se convierte desde Unix timestamp a tipo datetime.
- ``feed_last_updated`` se convierte a datetime.
- Se agrega ``ingestion_timestamp`` con el momento en que el pipeline ejecuta la extraccion.
- Se agrega extraction_date para facilitar analisis por fecha.
### Validacion de disponibilidad
- Se marca is_invalid_availability cuando existen valores nulos o negativos en:
    - num_bikes_available
    - num_docks_available
### Validacion operativa de estacion
- Se marca is_station_unavailable cuando alguna de las banderas operativas no esta activa:
    - is_installed != 1
    - is_renting != 1
    - is_returning != 1

### Deduplicacion
Se deduplica por:
```
station_id + feed_last_updated
```
Esto evita duplicar la misma fotografia del estado de una estacion cuando el feed no cambio entre ejecuciones.

## Propuesta de guardado en Bronze

La informacion procesada se guarda en el bucket Bronze en formato Parquet.

Ruta seleccionada:
```
bck-bronze/ecobici/station_status/data.parquet
```

Se eligio esta ruta porque el requerimiento indica que en cada ruta debe permanecer un solo archivo Parquet usando logica de append.

Para cumplirlo, el pipeline hace lo siguiente:

1. Consulta el feed actual de ECOBICI.
2. Convierte los datos a DataFrame con Polars.
3. Lee el Parquet existente en Bronze si ya existe.
4. Concatena los datos anteriores con la nueva extraccion.
5. Deduplica por station_id y feed_last_updated.
6. Reescribe un unico archivo Parquet consolidado.

De esta manera, la ruta mantiene siempre un solo archivo:
```
data.parquet
```

## Criterio de particion

Para esta primera version no se aplico una particion fisica por fecha en la ruta.

Se eligio una ruta unica:
```
bck-bronze/ecobici/station_status/data.parquet
```
Esta decision simplifica la disponibilidad en Trino, ya que la tabla externa puede apuntar directamente al directorio:
```
s3a://bck-bronze/ecobici/station_status/
```

Si se usaran particiones fisicas como:
```
year=1998/month=05/dat=08
```

habria que considerar el manejo de particiones en Trino/Hive Metastore. Para un ambiente productivo, particionar por fecha podria mejorar el rendimiento de consultas historicas, pero tambien agregaria complejidad en el registro y mantenimiento de particiones.

Para este ejercicio adicional, la ruta unica facilita la validacion y cumple con el requisito de mantener un solo Parquet por ruta.


## Disponibilizacion en Trino

El pipeline crea una tabla externa en Trino apuntando a la ruta Bronze del dataset ECOBICI.

Tabla creada:
```
bronze.prueba.tbl_ecobici_station_status
```

Query puede probar con la siguiene query una vez finaliza correctamente el proceso en airflow
```sql
SELECT
    station_id,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    feed_last_updated,
    ingestion_timestamp,
    extraction_date,
    is_invalid_availability,
    is_station_unavailable
FROM bronze.prueba.tbl_ecobici_station_status
ORDER BY feed_last_updated DESC, station_id
LIMIT 50;
```



# Preguntas del ejercicio adicional
## 1. ¿Que dataset se selecciono para tu flujo?

Se selecciono el feed station_status de ECOBICI CDMX, disponible mediante el estandar GBFS

Este dataset contiene el estado operativo y la disponibilidad actual de las estaciones. Permite conocer cuantas bicicletas hay disponibles, cuantos espacios libres existen para devolver bicicletas y si la estacion esta habilitada para rentar o recibir bicicletas

Se eligio porque es un dataset dinamico y adecuado para un pipeline programado por tiempo.

## 2. ¿Que temporalidad se realizara la extraccion? Explica por que se selecciono este timing.

La extraccion se realiza cada 10 minutos.

Se selecciono esta frecuencia porque la disponibilidad de bicicletas y espacios puede cambiar rapidamente. Un intervalo de 10 minutos permite capturar cambios operativos relevantes sin generar demasiadas ejecuciones ni demasiadas escrituras en el ambiente local.

Una frecuencia de 5 minutos daria mas detalle, pero aumentaria la carga del pipeline. Una frecuencia de 30 minutos reduciria la carga, pero podria perder eventos cortos como estaciones vacias, saturadas o temporalmente no disponibles.

## 3. - ¿Que limpieza de datos usaste o crees que necesitaba los datos?

Se aplicaron las siguientes limpiezas y validaciones:

- Conversion de ``station_id`` a string.
- Conversion de campos numericos a enteros.
- Conversion de timestamps Unix a datetime.
- Creacion de ``ingestion_timestamp``.
- Creacion de ``extraction_date``.
- Validacion de valores negativos o nulos en disponibilidad.
- Creacion de la bandera ``is_invalid_availability``.
- Validacion de banderas operativas de la estacion.
- Creacion de la bandera ``is_station_unavailable``.
- Deduplicacion por ``station_id`` y ``feed_last_updated``.

No se corrigen valores de negocio sin una regla validada. Las inconsistencias detectadas se marcan con columnas booleanas para facilitar revision y analisis.

## 4. ¿Que propuesta de particion de ruta elegiste para el guardado de tu parquet y crees que esta particion afecta a Trino para su disponibilizacion automatica de datos?

Se eligio una ruta unica sin particiones fisicas:
```
bck-bronze/ecobici/station_status/data.parquet
```
Esta propuesta permite cumplir con el requisito de mantener un solo archivo Parquet por ruta usando logica de append.

En esta version no se particiono por fecha porque eso agregaria complejidad en Trino/Hive Metastore. Si se usan particiones fisicas, Trino puede requerir registro o sincronizacion de particiones dependiendo de la configuracion del conector Hive.

Para este ejercicio se priorizo una ruta simple y estable para facilitar la consulta desde DBeaver.


### ¿De todo el proceso, cual fue el reto mas grande?

El reto mas grande fue normalizar la creacion de DAGs mediante un patron de fabrica. La solucion debía permitir agregar nuevos pipelines sin duplicar codigo ni meter logica de negocio dentro del factory.

Para resolverlo, el factory se diseño como una capa generica que lee archivos `pipeline.json`, carga dinamicamente el modulo de procesamiento y crea las tareas segun la configuracion. Esto permite que futuros DAGs sean mas faciles de mantener y extender.

En cuanto a calidad de datos, el reto fue no contar con reglas de negocio definidas. Por ello, no se eliminaron registros inconsistentes ni se aplicaron correcciones asumidas. Las inconsistencias fueron marcadas con flags y enviadas a una salida de calidad, conservando la trazabilidad del dataset.