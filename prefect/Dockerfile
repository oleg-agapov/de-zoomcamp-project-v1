FROM prefecthq/prefect:2.8.4-python3.9

WORKDIR /opt/prefect

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .
