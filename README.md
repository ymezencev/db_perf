# AsyncPG Performance Comparison Repository

This repository contains a benchmarking suite for comparing the performance of various PostgreSQL client libraries in Python with async capabilities. 
It aims to provide an insight into the execution speed of different libraries when running a high number of queries concurrently.

## What's Inside

Each client library is tested with 50,000 select queries to provide meaningful performance data.

## Results

| Library                                    | Time (s)  |
|--------------------------------------------|-----------|
| psqlpy (raw)                               | 11.08     |
| psqlpy (With SQLAlchemy as query builder)  | 15.32     |
| asyncpg (raw)                              | 16.51     |
| asyncpgsa (SQLAlchemy wrapper for asyncpg) | 20.91     |
| asyncpg (With SQLAlchemy as query builder) | 20.97     |
| SQLAlchemy Async Session                   | 24.58     |
