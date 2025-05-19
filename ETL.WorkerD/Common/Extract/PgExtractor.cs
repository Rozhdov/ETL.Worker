using Dapper;
using Npgsql;
using System.Runtime.CompilerServices;

namespace ETL.WorkerD.Common.Extract;

public class PgExtractor<TExtract>([FromKeyedServices(ConnectionType.Extract)]NpgsqlConnection conn, PgMapper<TExtract> mapper) : IExtractor<TExtract>
{
    public required string Query { get; set; }

    public async IAsyncEnumerable<TExtract> ExtractAsync(long changeVersion, [EnumeratorCancellation] CancellationToken ct)
    {
        var reader = await conn.ExecuteReaderAsync(Query, new { ChangeVersion = changeVersion });

        if (!await reader.ReadAsync(ct))
        {
            yield break;
        }

        var mapFunc = mapper.GetMapFunction(reader);

        do
        {
            yield return mapFunc(reader);
        } while (await reader.ReadAsync(ct));
    }
}