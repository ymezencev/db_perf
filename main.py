import asyncio
import time

import asyncpg
import asyncpgsa
import psqlpy
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_asyncio
from sqlalchemy.dialects import postgresql as pg_dialects


pg_tables = sa.Table(
    "pg_tables",
    sa.MetaData(),
    sa.Column("schemaname"),
    sa.Column("tablename"),
    sa.Column("tableowner"),
    sa.Column("tablespace"),
    sa.Column("hasindexes"),
)
query = pg_tables.select().where(pg_tables.c.schemaname == "pg_catalog")
query_str, query_param = "select * from pg_tables where pg_tables.schemaname = $1::TEXT", "pg_catalog"

COUNT_QUERIES = 50_000
DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
SQLALCHEMY_DSN = "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
results = dict()


async def execute_asyncpgsa():
    pool = await asyncpgsa.create_pool(dsn=DSN)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        async with pool.acquire() as conn:
            await conn.execute(query)
    process_time = time.perf_counter() - start_time
    results["asyncpgsa"] = process_time
    await pool.close()


async def execute_asyncpg_sqla():
    pool = await asyncpg.create_pool(dsn=DSN)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        compiled = query.compile(dialect=pg_dialects.asyncpg.dialect())
        params = list(compiled.params[key] for key in compiled.positiontup)
        async with pool.acquire() as conn:
            await conn.execute(compiled.string, *params)
    process_time = time.perf_counter() - start_time
    results["asyncpg sqla"] = process_time
    await pool.close()


async def execute_raw_asyncpg():
    pool = await asyncpg.create_pool(dsn=DSN)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        async with pool.acquire() as conn:
            await conn.execute(query_str, query_param)
    process_time = time.perf_counter() - start_time
    results["asyncpg raw"] = process_time
    await pool.close()


async def execute_raw_psqlpy():
    pool = psqlpy.PSQLPool(dsn=DSN)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        await pool.execute(query_str, [query_param])
    process_time = time.perf_counter() - start_time
    results["psqlpy raw"] = process_time


async def execute_psqlpy_sqla():
    pool = psqlpy.PSQLPool(dsn=DSN)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        compiled = query.compile(dialect=pg_dialects.asyncpg.dialect())
        params = list(compiled.params[key] for key in compiled.positiontup)
        await pool.execute(compiled.string, params)
    process_time = time.perf_counter() - start_time
    results["psqlpy sqla"] = process_time


async def execute_sqlalchemy_session():
    engine = sa_asyncio.create_async_engine(SQLALCHEMY_DSN)
    async_session = sa_asyncio.async_sessionmaker(engine)
    start_time = time.perf_counter()
    for _ in range(COUNT_QUERIES):
        async with async_session() as session:
            (await session.execute(query)).scalars().all()
    process_time = time.perf_counter() - start_time
    results["sqlalchemy_session"] = process_time


if __name__ == "__main__":
    asyncio.run(execute_raw_asyncpg())
    asyncio.run(execute_raw_psqlpy())
    asyncio.run(execute_asyncpgsa())
    asyncio.run(execute_asyncpg_sqla())
    asyncio.run(execute_psqlpy_sqla())
    asyncio.run(execute_sqlalchemy_session())
    for k, v in sorted(results.items(), key=lambda x: x[1]):
        print(f"{k}: {v}")
