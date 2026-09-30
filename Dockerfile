# Image used for both locust-master and locust-worker in k8s/ -- the only
# difference between the two is the container command (--master vs
# --worker), set in their respective Deployment manifests.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY benchmarks/ benchmarks/
COPY config/ config/

# Master: web UI + worker-coordination ports. Workers connect out to the
# master on 5557 (and don't need any ports of their own exposed).
EXPOSE 8089 5557 5558

ENTRYPOINT ["python", "-m", "locust", "-f", "benchmarks/locustfile.py"]
