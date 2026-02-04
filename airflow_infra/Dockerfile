# Usa a imagem oficial do Airflow como base
FROM apache/airflow:2.8.1

# Copia o arquivo de requisitos para dentro do container
COPY requirements.txt /requirements.txt

# Instala as bibliotecas extras (dbt, pandas, drivers)
RUN pip install --no-cache-dir -r /requirements.txt