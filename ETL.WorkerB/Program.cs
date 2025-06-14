using Dapper;
using ETL.WorkerB;
using ETL.WorkerB.Common;
using ETL.WorkerB.Extensions;
using ETL.WorkerB.Implementation.Example1;
using Npgsql;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddNpgsqlDataSource(
    builder.Configuration.GetValue<string>("PostgresLockConnection")!, serviceKey: ConnectionType.Lock);
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

app.MapPatch("/etl/{key}/resetLock", async (string key, [FromKeyedServices(ConnectionType.Lock)] NpgsqlConnection conn) =>
{
    await conn.ExecuteAsync(
        """
        UPDATE public.lock_table
        SET is_running = false,
            change_version = 0
        WHERE key = @key;
        """, new { key });
});

app.MapPost("/prepare/{size:int}", async (int size,
    [FromKeyedServices(ConnectionType.Source)] NpgsqlConnection eConn,
    [FromKeyedServices(ConnectionType.Target)] NpgsqlConnection lConn,
    [FromKeyedServices(ConnectionType.Lock)] NpgsqlConnection lockConn) =>
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

    await lockConn.ExecuteAsync(
    """
        drop table if exists public.lock_table cascade;
        
        create table public.lock_table
        (
            key varchar(1024) not null primary key,
            change_version bigint not null,
            is_running boolean not null
        );
        
        insert into public.lock_table(key, change_version, is_running)
        values ('Example1', 0, false);
        """);
});

app.MapGet("/db-stats", async ([FromKeyedServices(ConnectionType.Source)] NpgsqlConnection eConn,
    [FromKeyedServices(ConnectionType.Target)] NpgsqlConnection lConn,
    [FromKeyedServices(ConnectionType.Lock)] NpgsqlConnection lockConn) =>
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

    var lockRes = await lockConn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        UNION ALL
        SELECT 'lock_table data:' AS info
        UNION ALL
        SELECT CONCAT_WS(', ', key, change_version, is_running) AS info
        FROM public.lock_table)
        """);

    return TypedResults.Ok(eRes.Append("------------").Concat(lRes).Append("------------").Concat(lockRes));
});

app.Run();