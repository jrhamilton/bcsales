FROM prefecthq/prefect:2.7.7-python3.9

RUN apt-get update -y \
	&& apt-get -y install wget zip unzip vim less

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host ppi.python.org --no-cache-dir

RUN mkdir -p /opt/prefect/flows
RUN mkdir -p /opt/prefect/sales

COPY flows /opt/prefect/flows
