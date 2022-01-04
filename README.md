# ARCHE-core

[![Latest Stable Version](https://poser.pugx.org/acdh-oeaw/arche-core/v/stable)](https://packagist.org/packages/acdh-oeaw/arche-core)
![Build status](https://github.com/acdh-oeaw/arche-core/workflows/phpunit/badge.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/acdh-oeaw/arche-core/badge.svg?branch=master)](https://coveralls.io/github/acdh-oeaw/arche-core?branch=master)
[![License](https://poser.pugx.org/acdh-oeaw/arche-core/license)](https://packagist.org/packages/acdh-oeaw/arche-core)

The core component of the ARCHE repository solution responsible for the CRUD operations and transaction support.

## Installation

`composer require acdh-oeaw/arche-core`

## Deployment

See https://github.com/acdh-oeaw/arche-docker

### Environment for development

An environment allowing you to edit code in your host system and run all the tests inside a docker container.

* Clone this repo and enter it
  ```bash
  git clone https://github.com/acdh-oeaw/arche-core.git
  cd arche-core
  ```
* Get all dependencies
  ```bash
  composer update
  ```
* Build the doker image with the runtime environment
  ```bash
  docker build -t arche-dev build/docker
  ```
* Run the runtime environment mounting the repository dir into it and wait until it's ready
  ```bash
  docker run --name arche-dev -v `pwd`:/var/www/html -e USER_UID=`id -u` -e USER_GID=`id -g` -d arche-dev
  docker logs -f arche-dev
  ```
  wait until you see (timestamps will obviously differ):
  ```
  2020-06-04 14:06:52,309 INFO success: apache2 entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
  2020-06-04 14:06:52,309 INFO success: postgresql entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
  2020-06-04 14:06:52,309 INFO success: rabbitmq entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
  2020-06-04 14:06:52,309 INFO success: tika entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
  ```
  then hit `CTRL+C`
* Enter the docker container and run tests inside it
  ```bash
  docker exec -ti -u www-data arche-dev /bin/bash
  ```
  and then inside the container
  ```bash
  XDEBUG_MODE=coverage vendor/bin/phpunit
  ```

## REST API documentation

https://app.swaggerhub.com/apis/zozlak/arche

## Architecture

![architecture](https://acdh-oeaw.github.io/arche-docs/diagrams/arche-core.png)

## Database structure

The main table is the `resources` one. It stores a list of all repository resources identified by their internal repo id (the `id` column) as well as transactions handling related data (columns `transaction_id` and `state`).

Metadata are devided into three tables according to the consistency checks applying to them.

* The `identifiers` table stores resources' identifiers (the repository assumes every resource may have many). The table enforces global identifiers uniquness. The RDF property storing the identifier comes implicitly from the repository's `config.yaml` (`$.schema.id`) and is not explicitly stored inside the database.
* The `relations` table stores all RDF triples having an URI as an object. It enforces (with a foreign key check) existence of a repository resource an RDF triple points to.
* The `metadata` table stores all other RDF triples. This table puts no constraints on the data. Triples are stored in an RDF-like way - each row in the table represents a single triple.
    * For triple values which look like a proper number/date the `value_n`/`value_t` column stores a value casted to number/timestamp. This allows for correct comparisons which would fail against string values.
    * The index on the `value` column is set up only on first 1000 characters of the value. This is both for technical and performance reasons. An important consequence is that **if you want to benefit from indexed search on the value column**, you should state your condition as `substring(value, 1, 1000) = 'yourValue'`.

Supplementary tables include:

* The `transactions` table which stores information about pending transactions.
* The `metadata_history` table which stores history of metadata modification. It's automatically filled in using triggers on tables `identifiers`, `relations` and `metadata`.
* The `full_text_search` table storing a GIST index on a tokenized metadata values and resources' text content allowing for a full text search (see the [Postgresql documentation](https://www.postgresql.org/docs/current/textsearch.html)).
* The `raw` table is used only for data migration from the previous ACDH-CH repository solution.

### Helper functions and views

* The `metadata_view` gathers together triples from both `identifiers`, `relations` and `metadata` tables.
* The `get_relatives()` function allows easy finding of resources related to a given one with a given RDF property. Internally it uses a recursive query which could be difficult to write correctly on you own.
* The `get_neighbors_metadata()` and the `get_relatives_metadata()` functions allow for easy fetching of metadata triples of bot a given resource and resources related to it. Either by any single-hop RDF property (`get_neighbors_metadata()`) or with any number of hops of a one selected metadata property (`get_relatives_metadata()`). 
