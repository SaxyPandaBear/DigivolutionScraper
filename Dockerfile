# Self-contained image: Airflow + this DAG's extra deps + the DAG source
# itself all baked in at build time, so a resource-constrained host only
# ever has to pull/run one image - no bind mounts, no installing packages
# on every container start.
FROM apache/airflow:3.3.0

USER airflow

COPY requirements-docker.txt /requirements-docker.txt
RUN pip install --no-cache-dir -r /requirements-docker.txt

COPY --chown=airflow:root digivolution_dag.py /opt/airflow/dags/digivolution_dag.py
COPY --chown=airflow:root evolutions.py /opt/airflow/dags/evolutions.py
COPY --chown=airflow:root modes.py /opt/airflow/dags/modes.py
COPY --chown=airflow:root names.py /opt/airflow/dags/names.py
COPY --chown=airflow:root tasks/ /opt/airflow/dags/tasks/

CMD ["standalone"]
