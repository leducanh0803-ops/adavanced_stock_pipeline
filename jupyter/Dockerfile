FROM apache/spark:3.5.1

USER root

RUN curl -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.91.3.jar \
        https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.91.3/nessie-spark-extensions-3.5_2.12-0.91.3.jar && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    pip install jupyterlab pyspark==3.5.1

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", \
     "--NotebookApp.token=''", "--notebook-dir=/opt/spark/work"]