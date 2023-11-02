# DuckDB OML Extension
This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

The OML extension let's you load OML files directly into DuckDB.

Use `Power_Consumption_load(filename)` to load OML data from a given OML file into a predetermined table with the following schema:
```sql
CREATE TABLE IF NOT EXISTS Power_Consumption (
    experiment_id VARCHAR,
    node_id VARCHAR,
    node_id_seq VARCHAR,
    time_sec VARCHAR NOT NULL,
    time_usec VARCHAR NOT NULL,
    power REAL NOT NULL,
    current REAL NOT NULL,
    voltage REAL NOT NULL
);
``` 

Alternatively the function `OmlGen(filename)` can be used to dynamically create a table with a schema determined by the metadata of the OML file. The data of the OML file will then be loaded into the new table.

## Building the Extension

The extension can be build by running the command:
```
make
```

In order to enable debugging, the following command should be run instead:
```
make debug
```
