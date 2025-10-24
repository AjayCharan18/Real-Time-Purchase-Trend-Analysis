FROM apache/spark:3.5.2



USER root

# Install PySpark matching the Spark runtime
RUN pip install --no-cache-dir pyspark==3.5.2

WORKDIR /opt/spark-apps

CMD ["bash"]
