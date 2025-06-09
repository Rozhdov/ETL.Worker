using Dapper;
using ETL.WorkerA.Common.Extract;
using Npgsql;

namespace ETL.WorkerA.Implementation.Example1;

public class ExampleExtractor(NpgsqlConnection conn) : IExtractor<ExtractModel>
{
    public async Task<IReadOnlyCollection<ExtractModel>> ExtractAsync(long changeVersion)
    {
        var result = await conn.QueryAsync<ExtractModel>(
            """
            SELECT key1, col1
            FROM public.source_table1
            WHERE key1 > @ChangeVersion
            """, new {ChangeVersion = changeVersion});

        return result.ToList();
    }
}