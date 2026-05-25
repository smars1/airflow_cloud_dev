## Ejercicio adicional - Pipeline ECOBICI

Para el ejercicio adicional se implemento un nuevo DAG utilizando el mismo stack del ejercicio principal:

- Apache Airflow como orquestador.
- Polars como motor de procesamiento.
- MinIO como Data Lake compatible con S3.
- Trino como motor SQL para consulta.
- DBeaver como cliente de validacion.

El objetivo fue demostrar que el patron de fabrica de DAGs permite agregar nuevos pipelines sin modificar la logica base del factory. Para este caso, se agrego un nuevo pipeline independiente llamado:

```text
ecobici_station_status_pipeline