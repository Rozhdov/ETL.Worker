﻿using Dapper;
using ETL.WorkerB.Common.Extract;
using Npgsql;

namespace ETL.WorkerB.Implementation.Example;

public class ExampleExtractor(NpgsqlConnection conn) : IExtractor<ExtractModel>
{
    public IAsyncEnumerable<ExtractModel> ExtractAsync(long changeVersion)
    {
        return conn.QueryUnbufferedAsync<ExtractModel>(
            """
            SELECT key1, col1
            FROM public.source_table1
            WHERE key1 > @ChangeVersion
            ORDER BY key1
            """, new {ChangeVersion = changeVersion});
    }
}