using Dapper;
using Npgsql;

namespace ETL.WorkerC.Common.Extract;

public class PgExtractor<TExtract>(NpgsqlConnection conn)
    : IExtractor<TExtract>
{
    public required string Query { get; set; }
    
    public IAsyncEnumerable<TExtract> ExtractAsync(long changeVersion)
    {
        return conn.QueryUnbufferedAsync<TExtract>(Query, new {ChangeVersion = changeVersion});
    }
}