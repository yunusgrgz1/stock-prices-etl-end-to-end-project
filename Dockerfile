FROM apache/airflow:2.6.0-python3.9

COPY requirements.txt /tmp/requirements.txt

USER root

# Java 17 
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java ortam değişkenleri
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# airflow 
USER airflow

# Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["python", "exchange.py"]