FROM python:3.10-slim-buster


WORKDIR /usr/src/dbt/dbt_project

COPY requirements.txt /usr/src/dbt/dbt_project
# Install the dbt Postgres adapter. This step will also install dbt-core
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install dbt dependencies (as specified in packages.yml file)
# Build seeds, models and snapshots (and run tests wherever applicable)
CMD dbt deps && dbt build --profiles-dir ./ && sleep infinity
