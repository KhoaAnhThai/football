FROM python:3.10-slim-buster


WORKDIR /usr/src/streamlit

COPY requirements.txt $DAGSTER_HOME

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD [ "streamlit", "run", "app.py" ]