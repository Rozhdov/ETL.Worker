[![.NET](https://github.com/Rozhdov/ETL.Worker/actions/workflows/dotnet.yml/badge.svg)](https://github.com/Rozhdov/ETL.Worker/actions/workflows/dotnet.yml)

# ETL.Worker

This repository contains code that supplements my SoftServe Engineering Community speech.

## Contents

**ETL.WorkerA** contains basic naive worker implementation.

**ETL.WorkerB** has performance and locking improvements.

In **ETL.WorkerC** we improved throughput, and made worker easier to extend with additional processes.

**ETL.WorkerC.Benchmarks** contains benchmarks used to decide on our avenues for performance improvements.

**ETL.WorkerD** scores reliability improvements, and example application of Expression Trees and Incremental Source Generators.

**ETL.WorkerD.Gen** and **ETL.WorkerD.Gen.Tests** are project with source generator and it's tests.

## How to Build and Run

- Make sure that .NET SDK 9 is installed.
- Deploy PostgreSQL 17 or later.
- Replace connection strings in appsettings.json files with your PostgreSQL connection.
- Run using provided launchSettings.json profiles.

## Available Endpoints

Use `/prepare/{size}` to create and seed tables with given number of rows. Use `/etl/{key}/resetLock` to reset lock for given key. Use `/etl/{key}/process` to execute ETL process with a given key. Use `/db-stats` to see important stats and check data in lock, source and target tables. 

> NOTE: stats may be updated with some delay.
