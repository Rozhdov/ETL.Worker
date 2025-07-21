using Dapper;
using ETL.WorkerA.Common.Load;
using Npgsql;

namespace ETL.WorkerA.Implementation.Example1;

public class Loader([FromKeyedServices(ConnectionType.Target)] NpgsqlConnection conn): ILoader<LoadModel>
{
    public async Task LoadAsync(IReadOnlyCollection<LoadModel> collection)
    {
        await conn.ExecuteAsync(
            """
            INSERT INTO public.target_table1 (key1, col1)
            VALUES (@key1, @col1)
            ON CONFLICT (key1) DO UPDATE
            SET col1 = @col1;
            """, collection);
    }
}