using Dapper;
using ETL.WorkerA;
using ETL.WorkerA.Common;
using ETL.WorkerA.Common.Lock;
using ETL.WorkerA.Extensions;
using ETL.WorkerA.Implementation.Example1;
using Npgsql;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresSourceConnection")!, serviceKey: ConnectionType.Source);
builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresTargetConnection")!, serviceKey: ConnectionType.Target);

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

app.MapPost("/prepare/{size:int}", async (int size,
    [FromKeyedServices(ConnectionType.Source)] NpgsqlConnection eConn,
    [FromKeyedServices(ConnectionType.Target)] NpgsqlConnection lConn) =>
{
    await eConn.ExecuteAsync(
        """
        drop table if exists public.source_table1 CASCADE;

        create table public.source_table1
        (
            key1 bigint generated always as identity
                constraint source_table1_pk
                    primary key,
            col1 varchar(1024) not null
        );

        alter table public.source_table1
            owner to postgres;

        insert into source_table1(col1)
        select md5(random()::text)
        from generate_series(1,@size) id;

        drop table if exists public.source_table2 CASCADE;
        """, new { size }, commandTimeout: 120);

    await lConn.ExecuteAsync(
        """
        drop table if exists public.target_table1 CASCADE;

        create table public.target_table1
        (
            key1 bigint constraint target_table1_pk
                    primary key,
            col1 varchar(1024) not null
        );
        
        alter table public.target_table1
            owner to postgres;

        drop table if exists public.target_table2 CASCADE;
        """);
});

app.MapGet("/db-stats", async ([FromKeyedServices(ConnectionType.Source)] NpgsqlConnection eConn,
    [FromKeyedServices(ConnectionType.Target)] NpgsqlConnection lConn) =>
{
    var eRes = await eConn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        UNION ALL
        SELECT 'source_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.source_table1 ORDER BY key1 DESC LIMIT 5))
        """);

    var lRes = await lConn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        UNION ALL
        SELECT 'target_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.target_table1 ORDER BY key1 DESC LIMIT 5))
        """);

    return TypedResults.Ok(eRes.Append("------------").Concat(lRes));
});

app.Run();