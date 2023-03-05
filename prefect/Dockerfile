FROM prefecthq/prefect:2.8.4-python3.9

ARG prefect_api_key
ARG prefect_api_url

ENV PREFECT_API_KEY=$prefect_api_key
ENV PREFECT_API_URL=$prefect_api_url

ENV POOL=default-agent-pool
ENV WORK_QUEUE=default

WORKDIR /opt/prefect

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

ENTRYPOINT [ "prefect", "agent", "start", "--pool", "default-agent-pool", "--work-queue", "default" ]
