FROM quay.io/jupyter/pyspark-notebook
WORKDIR /app
COPY  requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt