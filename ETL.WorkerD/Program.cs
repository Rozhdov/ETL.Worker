using Dapper;
using ETL.WorkerD;
using ETL.WorkerD.Common;
using ETL.WorkerD.Extensions;
using ETL.WorkerD.Implementation.Example1;
using ETL.WorkerD.Implementation.Example2;
using Npgsql;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresConnection")!);
builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresExtractConnection")!, serviceKey: ConnectionType.Extract);
builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresLoadConnection")!, serviceKey: ConnectionType.Load);

builder.Services.AddOpenApi();

builder.Services.AddEtl();
builder.Services.AddExample1();
builder.Services.AddExample2();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

app.MapPost("/etl/{key}/process", async (string key, Etl etl, CancellationToken ct) =>
{
    await etl.ProcessAsync(key, ct);
});

app.MapPatch("/etl/{key}/resetLock", async (string key, NpgsqlConnection conn) =>
{
    await conn.ExecuteAsync(
        """
        UPDATE public.lock_table
        SET is_running = false,
            change_version = 0,
            lock_expiration = null
        WHERE key = @key;
        """, new { key });
});

app.MapPost("/prepare/{size:int}", async (int size, NpgsqlConnection conn) =>
{
    await conn.ExecuteAsync(@"
drop table if exists public.source_table1 CASCADE;
drop table if exists public.target_table1 CASCADE;

create table public.source_table1
(
    key1 bigint generated always as identity
        constraint source_table1_pk
            primary key,
    col1 varchar(1024) not null
);

alter table public.source_table1
    owner to postgres;

create table public.target_table1
(
    key1 bigint constraint target_table1_pk
            primary key,
    col1 varchar(1024) not null
);

alter table public.target_table1
    owner to postgres;

insert into source_table1(col1)
select md5(random()::text)
from generate_series(1,@size) id;

drop table if exists public.source_table2 CASCADE;
drop table if exists public.target_table2 CASCADE;

create table public.source_table2
(
    version bigint generated always as identity,
    key1 varchar(1024) not null,
    key2 varchar(1024) not null,
    col1 integer,
    col2 integer not null,
    col3 varchar(1024),
    col4 timestamp not null,
    col5 timestamp,
    PRIMARY KEY(version)
);

alter table public.source_table2
    owner to postgres;

create table public.target_table2
(
    key1 varchar(1024) not null,
    key2 varchar(1024) not null,
    col1 integer,
    col2 integer not null,
    col3 varchar(1024),
    col4 timestamp not null,
    col5 timestamp,
    PRIMARY KEY(key1, key2)
);

alter table public.target_table2
    owner to postgres;

insert into source_table2(key1, key2, col1, col2, col3, col4, col5)
select md5(random(0, 1000)::text),
       md5(random(0, 1000)::text),
       NULLIF(random(0, 100), 50),
       random(0, 100),
       md5(random(0, 1000)::text),
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP
from generate_series(1,@size) id;

drop table if exists public.lock_table cascade;

create table public.lock_table
(
    key varchar(1024) not null primary key,
    change_version bigint not null,
    is_running boolean not null,
    lock_expiration timestamp with time zone null
);

insert into public.lock_table(key, change_version, is_running, lock_expiration)
values ('Example1', 0, false, null),
       ('Example2', 0, false, null);
", new {size}, commandTimeout: 120);
});

app.MapGet("/db-stats", async (NpgsqlConnection conn) =>
{
    var res = await conn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        ORDER BY relname)
        UNION ALL
        SELECT 'lock_table data:' AS info
        UNION ALL
        SELECT CONCAT_WS(', ', key, change_version, is_running, lock_expiration) AS info
        FROM public.lock_table
        UNION ALL
        SELECT 'source_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.source_table1 ORDER BY key1 DESC LIMIT 5)
        UNION ALL
        SELECT 'target_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.target_table1 ORDER BY key1 DESC LIMIT 5)
        UNION ALL
        SELECT 'source_table2 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, key2, col1, col2, col3, col4, col5) AS info FROM public.source_table2 ORDER BY key1 DESC, key2 DESC LIMIT 5)
        UNION ALL
        SELECT 'target_table2 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, key2, col1, col2, col3, col4, col5) AS info FROM public.target_table2 ORDER BY key1 DESC, key2 DESC LIMIT 5)
        """);

    return TypedResults.Ok(res);
});

app.Run();