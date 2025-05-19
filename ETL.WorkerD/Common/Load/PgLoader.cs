using Dapper;
using ETL.PgLoadGen;
using Npgsql;

namespace ETL.WorkerD.Common.Load;

public class PgLoader<TLoad>([FromKeyedServices(ConnectionType.Load)] NpgsqlConnection conn)
    : ILoader<TLoad>
{
    public required IPgCopier<TLoad> Copier { get; set; }
    
    public async Task LoadAsync(IReadOnlyCollection<TLoad> collection, CancellationToken ct)
    {
        if (collection.Count == 0)
        {
            return;
        }
        
        await conn.OpenAsync(ct);
        
        // Create temp table based on target table
        await conn.ExecuteAsync(Copier.CreateTempTableSql);
        
        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync(Copier.CopySql, ct))
        {
            // Perform COPY
            foreach (var model in collection)
            {
                await Copier.WriteAsync(writer, model, ct);
            }

            await writer.CompleteAsync(ct);
        }
        
        // Merge temp table into target table
        await conn.ExecuteAsync(Copier.UpsertSql);
        
        await conn.CloseAsync();
    }
}