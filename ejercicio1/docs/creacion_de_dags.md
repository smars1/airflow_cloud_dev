# Como crear un nuevo DAG

Para crear un nuevo DAG con la arquitectura de este proyecto debera definir lo siguientes puntos, 

1. Crear un subdirectorio en Airflow/dags/templates/<nuevo_pipeline>/
2. Crear pipeline.json.
3. Crear trino_tables.json.
4. Crear el modulo Python en Airflow/include/pipelines/<nuevo_pipeline>/
5. Definir config.py.
6. Definir schemas.py.
7. Definir process.py con una funcion callable.
8. Registrar el modulo y callable en pipeline.json.
9. Reiniciar Airflow dag-processor/scheduler.

## Ejemplo de la estructura que debe seguir
```sh
Airflow/dags/templates/my_nuevo_dag/
├── pipeline.json
└── trino_tables.json

Airflow/include/pipelines/my_nuevo_dag/
├── __init__.py
├── config.py
├── schemas.py
└── process.py
```


