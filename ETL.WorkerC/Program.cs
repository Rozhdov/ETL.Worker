using Dapper;
using ETL.WorkerC;
using ETL.WorkerC.Common.Processor;
using ETL.WorkerC.Extensions;
using ETL.WorkerC.Implementation.Example1;
using ETL.WorkerC.Implementation.Example2;
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
builder.Services.AddExample1();
builder.Services.AddExample2();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

app.MapPost("/etl/{key}/process", async (string key, IServiceProvider sp) =>
{
    var processor = sp.GetRequiredKeyedService<IProcessor>(key);
    await processor.ProcessAsync();
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
        drop table if exists public.source_table1 cascade;

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
        select repeat(md5(random()::text), 10)
        from generate_series(1,@size) id;

        drop table if exists public.source_table2 cascade;

        create table public.source_table2
        (
            version bigint generated always as identity,
            key1 char(32) not null,
            key2 char(32) not null,
            col1 integer,
            col2 integer not null,
            col3 varchar(1024),
            col4 timestamp not null,
            col5 timestamp,
            primary key(version)
        );

        alter table public.source_table2
            owner to postgres;

        insert into source_table2(key1, key2, col1, col2, col3, col4, col5)
        select md5(random(0, 1000)::text),
               md5(random(0, 1000)::text),
               nullif(random(0, 100), 50),
               random(0, 100),
               repeat(md5(random()::text), 10),
               current_timestamp - (random(1, 365) * interval '1 day'),
               case when random(0, 100) > 80 then current_timestamp else null end
        from generate_series(1,@size) id;
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

        create table public.target_table2
        (
            key1 char(32) not null,
            key2 char(32) not null,
            col1 integer,
            col2 integer not null,
            col3 varchar(1024),
            col4 timestamp not null,
            col5 timestamp,
            PRIMARY KEY(key1, key2)
        );
        
        alter table public.target_table2
            owner to postgres;
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
        values ('Example1', 0, false),
               ('Example2', 0, false);
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
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.source_table1 ORDER BY key1 DESC LIMIT 5)
        UNION ALL
        SELECT 'source_table2 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, key2, col1, col2, col3, col4, col5) AS info FROM public.source_table2 ORDER BY key1 DESC, key2 DESC LIMIT 5))
        """);

    var lRes = await lConn.QueryAsync<string>(
        """
        SELECT * FROM (SELECT CONCAT_WS('', schemaname, '.', relname, ' rows: ', n_live_tup, ', inserted: ', n_tup_ins, ', updated: ', n_tup_upd) AS info
        FROM pg_stat_user_tables
        UNION ALL
        SELECT 'target_table1 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, col1) AS info FROM public.target_table1 ORDER BY key1 DESC LIMIT 5)
        UNION ALL
        SELECT 'target_table2 data:' AS info
        UNION ALL
        SELECT * FROM (SELECT CONCAT_WS(', ', key1, key2, col1, col2, col3, col4, col5) AS info FROM public.target_table2 ORDER BY key1 DESC, key2 DESC LIMIT 5))
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