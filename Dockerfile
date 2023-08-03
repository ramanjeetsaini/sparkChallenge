# Use the official Spark image as the base image
FROM bitnami/spark

# Copy the Python script into the container
COPY . /opt/pokemon_analysis
WORKDIR /opt/pokemon_analysis
# Install modules
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Run the Spark job
CMD ["spark-submit", "--master", "spark://spark_master:7077", "--deploy-mode", "client", "/opt/pokemon_analysis/spark_task.py"]
