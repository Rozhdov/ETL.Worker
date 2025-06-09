using Dapper;
using ETL.WorkerB.Common.Load;
using Npgsql;
using NpgsqlTypes;

namespace ETL.WorkerB.Implementation.Example1;

public class ExampleLoader(NpgsqlConnection conn): ILoader<LoadModel>
{
    public async Task LoadAsync(IReadOnlyCollection<LoadModel> collection)
    {
        if (collection.Count == 0)
        {
            return;
        }
        
        await conn.OpenAsync();
        
        // Create temp table
        await conn.ExecuteAsync(
            """
            CREATE TEMP TABLE tmpt
            (
                key1 bigint not null,
                col1 varchar(1024) not null
            );
            """);
        
        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync("COPY tmpt (key1, col1) FROM STDIN (FORMAT BINARY);"))
        {
            // Perform COPY
            foreach (var model in collection)
            {
                await writer.StartRowAsync();
                // Provide as much type data as possible - use generic WriteAsync<T> and pass correct db type
                await writer.WriteAsync(model.key1, NpgsqlDbType.Bigint);
                await writer.WriteAsync(model.col1, NpgsqlDbType.Varchar);
            }

            await writer.CompleteAsync();
        }

        // Merge temp table into target table
        await conn.ExecuteAsync(
            """
            INSERT INTO public.target_table1 (key1, col1)
            SELECT key1, col1
            FROM tmpt
            ON CONFLICT (key1) 
            DO UPDATE SET col1 = excluded.col1;
            """);
        
        await conn.CloseAsync();
    }
}