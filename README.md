## DBT-Flink Adapter

This is POC for DBT-FLINK adapter

## rerequisities

* Flink 1.16+
* Flink SqlGateway

## Usage

Before we start to play with dbt adapter, we need to setup Flink cluster with SqlGateway configured and running.
To simplify that process we prepared Docker Compose configuration.

### Install `dbt-flink-adapter`

```bash
cd project_example
python3 -m venv venv
source venv/bin/activate
pip install dbt-flink-adapter
dbt --version
```
`dbt-flink` should be enlisted among plugins

### Configure DBT profile
Locate DBT profile on your machine.
It should be in home directory under `~/.dbt/profiles.yml`
Add there below config:
```yml
flink_profile:
  target: dev
  outputs:
    dev:
      type: flink
      host: localhost
      port: 8083
      session_name: test_session
```


### Launch Flink cluster

```bash
cd envs/kafka
docker compose up

cd envs/flink-1.16
docker compose up
```

### Play with sample DBT project with `dbt-flink` adapter
```bash
dbt test
dbt run
```

FLink SQL tables should be created on Flink cluster

### tear down Flink cluster

```bash
docker compose stop
```

### Creating DBT model

TODO

#### Source

##### Watermark

To provide watermark pass `column` and `strategy` reference under `watermark` key in config.

Example:
```yaml
sources:
  - name: my_source
    tables:
      - name: clickstream
        config:
          connector_properties:
            ...
          watermark:
            column: event_timestamp
            strategy: event_timestamp
        columns:
          - name: event_timestamp
            data_type: TIMESTAMP(3)
```

SQL passed to Flink will look like:
```sql
CREATE TABLE IF NOT EXISTS my_source (
    `<column>` TIMESTAMP(3),
    WATERMARK FOR <column> AS <strategy>
) WITH (
    ...
)
```

Please refer to Flink documentation about possible strategies: [flink-doc/watermark](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/create/#watermark)
