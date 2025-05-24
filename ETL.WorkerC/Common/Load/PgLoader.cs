using System.Reflection;
using Dapper;
using Npgsql;
using NpgsqlTypes;

namespace ETL.WorkerC.Common.Load;

public class PgLoader<TLoad>(NpgsqlConnection conn)
    : ILoader<TLoad>
{
    public required string TableName { get; set; }
    
    public async Task LoadAsync(IReadOnlyCollection<TLoad> collection)
    {
        if (collection.Count == 0)
        {
            return;
        }

        var propData = GetProperties();
        var columns = string.Join(", ", propData.Select(x => x.Name));
        
        await conn.OpenAsync();
        
        // Create temp table based on target table
        await conn.ExecuteAsync(
            $"""
            CREATE TEMP TABLE tmpt 
            AS SELECT {columns}
            FROM {TableName}
            LIMIT 0
            WITH NO DATA;
            """);
        
        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync($"COPY tmpt ({columns}) FROM STDIN (FORMAT BINARY);"))
        {
            // Perform COPY
            foreach (var model in collection)
            {
                await writer.StartRowAsync();
                foreach (var prop in propData)
                {
                    var value = prop.GetValue(model);
                    await writer.WriteAsync(value, prop.DbType);
                }
            }

            await writer.CompleteAsync();
        }
        
        // Merge temp table into target table
        var keys = string.Join(", ", propData.Where(x => x.IsKey).Select(x => x.Name));
        var setStatement = 
            string.Join(", ", propData.Where(x => !x.IsKey).Select(x => $"{x.Name} = excluded.{x.Name}"));
        await conn.ExecuteAsync(
            $"""
            INSERT INTO {TableName} ({columns})
            SELECT {columns}
            FROM tmpt
            ON CONFLICT ({keys}) 
            DO UPDATE SET {setStatement};
            """);
        
        await conn.CloseAsync();
    }

    private PropertyData[] GetProperties()
    {
        var props = typeof(TLoad).GetProperties();
        return props.Select(x => new PropertyData
        {
            Name = x.Name,
            IsKey = x.GetCustomAttribute<UpsertKeyAttribute>() is not null,
            GetValue = x.GetValue,
            DbType = x.PropertyType switch
            {
                { } type when type == typeof(int) || type == typeof(int?) => NpgsqlDbType.Integer,
                { } type when type == typeof(long) => NpgsqlDbType.Bigint,
                { } type when type == typeof(string) => NpgsqlDbType.Varchar,
                { } type when type == typeof(DateTime) || type == typeof(DateTime?) => NpgsqlDbType.Timestamp,
                _ => throw new InvalidOperationException()
            }
        }).ToArray();
    }

    private class PropertyData
    {
        public required string Name { get; init; }
        public bool IsKey { get; init; }
        public required Func<object?, object?> GetValue { get; init; }
        public NpgsqlDbType DbType { get; init; }
    }
}