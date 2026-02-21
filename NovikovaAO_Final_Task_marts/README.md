1. Первоначальная генерация данных с помощью скрипта generate_mongo_data.py:
![generate_data.png](https://github.com/Quidam33/ETL/blob/cfdfb5644f84ec63190bd2f22296d4709e57726a/NovikovaAO_Final_Task_marts/images/generate_data.png)

2. Загрузка данных в PostgreSQL с помощью mongo_to_postgres_dag.py:
![raw_data.png](https://github.com/Quidam33/ETL/blob/main/NovikovaAO_Final_Task_marts/images/raw_data.png)

3. Создание витрин - datamarts_dag.py:
![datamarts.png](https://github.com/Quidam33/ETL/blob/78d5f7045097c72cf67593216cfe481c1b82a03b/NovikovaAO_Final_Task_marts/images/datamarts.png)

4. Скрин отработавших дагов:
![airflow_dags.png](https://github.com/Quidam33/ETL/blob/78d5f7045097c72cf67593216cfe481c1b82a03b/NovikovaAO_Final_Task_marts/images/airflow_dags.png)
