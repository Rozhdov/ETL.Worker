using Dapper;
using ETL.WorkerA.Common;
using ETL.WorkerA.Common.Lock;
using ETL.WorkerA.Extensions;
using ETL.WorkerA.Implementation.Example1;
using Npgsql;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresConnection")!);

builder.Services.AddOpenApi();

builder.Services.AddEtl();

builder.Services.AddEtlProcess<ExtractModel, LoadModel>("Example1")
    .WithExtractor<ExampleExtractor>()
    .WithTransformer<ExampleTransformer>()
    .WithLoader<ExampleLoader>()
    .WithChangeVersion(x => x.key1)
    .Build();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

app.MapPost("/etl/{key}/process", async (string key, Etl etl) =>
{
    await etl.ProcessAsync(key);
});

app.MapPost("/etl/{key}/resetLock", (string key, ILock @lock) =>
{
    @lock.UpdateLock(key, 0);
    @lock.ReleaseLock(key);
});

app.MapPost("/prepare/{size:int}", async (int size, NpgsqlConnection conn) =>
{
    await conn.ExecuteAsync(@"
drop table if exists public.source_table1 CASCADE;
drop table if exists public.target_table1 CASCADE;
drop table if exists public.source_table2 CASCADE;
drop table if exists public.target_table2 CASCADE;
drop table if exists public.lock_table cascade;

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
from generate_series(1,@size) id;", new {size}, commandTimeout: 120);
});

app.MapGet("/db-stats", async (NpgsqlConnection conn) =>
{
    var res = await conn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        ORDER BY relname)
        UNION ALL
        SELECT 'source_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.source_table1 ORDER BY key1 DESC LIMIT 5)
        UNION ALL
        SELECT 'target_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.target_table1 ORDER BY key1 DESC LIMIT 5)
        """);

    return TypedResults.Ok(res);
});

app.Run();