# dbt Flink Adapter

[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/dbt-flink-adapter)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/dbt-flink-adapter.svg)](https://badge.fury.io/py/dbt-flink-adapter)
[![Downloads](https://pepy.tech/badge/dbt-flink-adapter)](https://pepy.tech/badge/dbt-flink-adapter)

This is an MVP of dbt Flink Adapter. It allows materializing of dbt models as Flink cluster streaming pipelines and batch jobs.

## Prerequisites

* Flink 1.16+ with Flink SQL Gateway
* Python 3.8+ with pip
* (Optionally) venv

## Setup

This adapter is connecting to Flink SQL Gateway which is not started in Flink by default.
Please refer to [flink-doc/starting-the-sql-gateway](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql-gateway/overview/#starting-the-sql-gateway)
on how to start SQL gateway in your cluster.

For testing and developing purposes you can use `envs/flink-1.16/docker-compose.yml` to start one node Flink cluster with SQL Gateway.

```shell
$ cd envs/flink-1.16
$ docker compose up
```

### Install `dbt-flink-adapter`

Create virtual environment and install `dbt-flink-adapter` from [PyPI/dbt-flink-adapter](https://pypi.org/project/dbt-flink-adapter/) with `pip`

```shell
$ python3 -m venv ~/.virtualenvs/dbt-example1
$ source ~/.virtualenvs/dbt-example1/bin/activate
$ pip3 install dbt-flink-adapter
$ dbt --version
...
Plugins:
  - flink: x.y.z
```

### Create and initialize dbt project

Navigate to directory in which you want to create your project. If you are using Flink with SQL Gateway started
from docker-compose.yml file in this repo you can leave all values as defaults.

```shell
$  dbt init
Enter a name for your project (letters, digits, underscore): example1
Which database would you like to use?
[1] flink

Enter a number: 1
host (Flink SQL Gateway host) [localhost]:
port [8083]:
session_name [test_session]:
database (Flink catalog) [default_catalog]:
schema (Flink database) [default_database]:
threads (1 or more) [1]:

$ cd example1
```

## Creating and running dbt model

On how to create and run dbt model please refer to [dbt-docs](https://docs.getdbt.com/docs/build/projects).
This README will focus on things that are specific for this adapter.

```shell
dbt run
```

### Source

In typical use-case dbt connects and runs its ETL processes on database engine that already has connection with underlying persistence layer.
In case of Flink however it's only a processing engine, and we need to define connectivity with external persistence.
To do so we have to define sources in our dbt model.

#### Connector properties

dbt-flink-adapter will read `config/connector_properties` key and use it as connector properties.

#### Type

Flink supports sources in batch and streaming mode, use `type` to select what execution environment will be used during source creation.

#### column type
current has these values, refer to [flink-doc/create-table](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/create/#create-table)
- physical (default)
- metadata
- computed

#### Watermark

To provide watermark pass `column` and `strategy` reference under `watermark` key in config.

Please refer to Flink documentation about possible watermark strategies: [flink-doc/watermark](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/create/#watermark)

#### Example

```yaml
sources:
  - name: my_source
    tables:
      - name: clickstream
        config:
          type: streaming
          connector_properties:
            connector: 'kafka'
            properties.bootstrap.servers: 'kafka:29092'
            topic: 'clickstream'
          watermark:
            column: event_timestamp
            strategy: event_timestamp
        columns:
          - name: id
            data_type: BIGINT
          - name: id2
            column_type: computed
            expression: id + 1
          - name: event_timestamp
            data_type: TIMESTAMP(3)
          - name: ts2
            column_type: metadata
            data_type: TIMESTAMP(3)
            expression: timestamp
```

SQL passed to Flink will look like:
```sql
CREATE TABLE IF NOT EXISTS my_source (
  `id` BIGINT,
  `id2` AS id + 1,
  `event_timestamp` TIMESTAMP(3),
  `ts2` TIMESTAMP(3) METADATA  FROM 'timestamp',
  WATERMARK FOR event_timestamp AS event_timestamp
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'kafka:29092',
  'topic' = 'clickstream'
)
```

### Model

This adapter currently supports two types of materialization *table* and *view*. Because in Flink table has to be
associated with a connector `type` and `connector_properties` have to be provided similar like in case of defining sources.

#### Example

`models.yml`

```yaml
models:
  - name: my_model
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        properties.bootstrap.servers: 'kafka:29092'
        topic: 'some-output'
```

`my_model.sql`

```sql
select *
from {{ source('my_source', 'clickstream') }}
where event = 'some-event'
```

### Seed

dbt-flink-adapter can use Flink to insert seed data in any Flink supported connector.
Similar like in case of sources and models you have to provide connector configuration.

#### Example

`seeds.yml`

```yaml
seeds:
  - name: clickstream
    config:
      connector_properties:
        connector: 'kafka'
        properties.bootstrap.servers: 'kafka:29092'
        topic: 'clickstream'
```

### Tests

Dbt also allows executing assertions in a form of tests to validate input data or model output if it does not contain abnormal values.
All generic tests are a select statement which is considered as passed when it did not found any rows.

The problem is how to define such thing in streaming pipeline? It is not possible to tell that in entire stream there are no such entries
as stream by definition is infinite. What we can do however is to have run the test for some specific time and if in that time there are no
abnormal values, test will be considered as passed.

To facilitate it dbt-flink-adapter when writing a sql query supports fetch_timeout_ms and mode directive.

```sql
select /** fetch_timeout_ms(5000) mode('streaming') */
  *
from {{ ref('my_model')}}
where
  event <> 'some-event'
```

In this example we are telling dbt-flink-adapter to fetch for 5 seconds in streaming mode.

### dbt_project.yml

You can extract common configurations of your model and sources into `dbt_project.yml` [dbt-docs/general-configuration](https://docs.getdbt.com/reference/model-configs#general-configurations).
If you define the same kay in `dbt_project.yml` and in your model or source dbt will always override entire key value.
In case you wish to extract some keys from under `connector_properties` you can specify configuration under `default_connector_properties`
which will get merged with `connection_properies`.

#### Example

`dbt_project.yml`

```yaml
models:
  example1:
    +materialized: table
    +type: streaming
    +default_connector_properties:
      connector: 'kafka'
      properties.bootstrap.servers: 'kafka:29092'

sources:
  example1:
    +type: streaming
    +default_connector_properties:
      connector: 'kafka'
      properties.bootstrap.servers: 'kafka:29092'

seeds:
  example1:
    +default_connector_properties:
      connector: 'kafka'
      properties.bootstrap.servers: 'kafka:29092'
```

`models.yml`

```yaml
models:
  - name: my_model
    config:
      connector_properties:
        topic: 'some-output'
```

`sources.yml`

```yaml
sources:
  - name: my_source
    tables:
      - name: clickstream
        config:
          connector_properties:
            topic: 'clickstream'
          watermark:
            column: event_timestamp
            strategy: event_timestamp
        columns:
          - name: event_timestamp
            data_type: TIMESTAMP(3)
```

`seeds.yml`

```yaml
seeds:
  - name: clickstream
    config:
      connector_properties:
        topic: 'clickstream'
```

## Sessions

Our interaction with Flink cluster is done in sessions any table and view created in one session will not be visible in another session.
Session by default is only valid for 10 minutes. Because of that if you will run dbt test after more than 10 minutes from dbt run
it will fail and in Flink logs you will find that it cannot find your tables. Currently, the only way to run this would be to rerun entire model.

Session handler is stored in `~/.dbt/flink-session.yml` file, if you want to force new session you can simply delete that file.

## Releasing

To release new version first execute [prepare-release](https://github.com/getindata/dbt-flink-adapter/actions/workflows/prepare-release.yml) action.
Please keep in mind that major and minor version have to be exactly the same as major and minor version of dbt-core.

This action will create a release branch with bumped version and changelog prepared for release. It will also open a Pull Request to main branch if everything is ok with it - merge it.

Next execute [publish](https://github.com/getindata/dbt-flink-adapter/actions/workflows/publish.yml) on branch that was just created by prepare-release action.
