FROM python:3.11-slim

WORKDIR /home/jupyter

RUN pip install --no-cache-dir \
    jupyterlab==4.3.6 \
    duckdb==1.5.2 \
    pandas==2.2.3 \
    boto3==1.35.0 \
    ipywidgets==8.1.5 \
    trino==0.330.0

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=", \
     "--ServerApp.password=", \
     "--notebook-dir=/home/jupyter"]