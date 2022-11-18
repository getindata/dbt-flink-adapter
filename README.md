## DBT-Flink Adapter

This is POC for DBT-FLINK adapter

## rerequisities

* Flink 1.16+
* Flink SqlGateway

## Usage

Before we start to play with dbt adapter, we need to setup Flink cluster with SqlGateway configured and running.
To simplify that process we prepared Docker Compose configuration.

### Install `dbt-flink` adapter

```bash
cd project_example
python3 -m venv venv
source venv/bin/activate
pip install dbt-materialize
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
