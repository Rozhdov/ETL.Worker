using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using Dapper;
using Npgsql;
using NpgsqlTypes;

namespace ETL.WorkerC.Benchmarks;

[MemoryDiagnoser]
[MaxIterationCount(30)]
[EtwProfiler]
public class PgCopyBenchmark
{
    private const string CreateTempTableSql =
        "CREATE TEMP TABLE tmpt AS SELECT key1, key2, col1, col2, col3, col4, col5 FROM public.target_table2 LIMIT 0 WITH NO DATA;";
    private const string CopySql = "COPY tmpt (key1, key2, col1, col2, col3, col4, col5) FROM STDIN (FORMAT BINARY);";

    private readonly NpgsqlDataSource _dataSource;
    private readonly NpgsqlDataSource _dataSource16XBuffer;

    private const int Count = 400_000;
    private readonly Model[] _data;
    private readonly Random _rnd = new();
    
    public PgCopyBenchmark()
    {
        _dataSource = NpgsqlDataSource.Create("User ID=postgres;Password=postgres;Host=192.168.0.177;Port=5432;");
        _dataSource16XBuffer = NpgsqlDataSource.Create("User ID=postgres;Password=postgres;Host=192.168.0.177;Port=5432;Write Buffer Size=131072");
        _data = Enumerable.Range(0, Count).Select(x => new Model
        {
            key1 = Guid.NewGuid().ToString(),
            key2 = Guid.NewGuid().ToString(),
            col1 = _rnd.Next(),
            col2 = _rnd.Next(),
            col3 = Guid.NewGuid().ToString(),
            col4 = DateTime.Now,
            col5 = DateTime.Now
        }).ToArray();
    }

    [Benchmark(Baseline = true)]
    public async Task ReflectionAsync()
    {
        var propData = GetProperties();
        var ct = CancellationToken.None;
        await using var conn = _dataSource.CreateConnection();
        await conn.OpenAsync(ct);

        // Create temp table based on target table
        await conn.ExecuteAsync(CreateTempTableSql);

        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync(CopySql, ct))
        {
            // Perform COPY
            foreach (var model in _data)
            {
                await writer.StartRowAsync(ct);
                foreach (var prop in propData)
                {
                    var value = prop.GetValue(model);
                    await writer.WriteAsync(value, prop.DbType, ct);
                }
            }

            await writer.CompleteAsync(ct);
        }

        await conn.CloseAsync();
    }

    [Benchmark]
    public async Task GeneratedAsync()
    {
        var ct = CancellationToken.None;
        await using var conn = _dataSource.CreateConnection();
        await conn.OpenAsync(ct);

        // Create temp table based on target table
        await conn.ExecuteAsync(CreateTempTableSql);

        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync(CopySql, ct))
        {
            // Perform COPY
            foreach (var model in _data)
            {
                await WriteAsync(writer, model, ct);
            }

            await writer.CompleteAsync(ct);
        }

        await conn.CloseAsync();
    }

    [Benchmark]
    public async Task Generated16XBufferAsync()
    {
        var ct = CancellationToken.None;
        await using var conn = _dataSource16XBuffer.CreateConnection();
        await conn.OpenAsync(ct);

        // Create temp table based on target table
        await conn.ExecuteAsync(CreateTempTableSql);

        // Initialize COPY
        await using (var writer = await conn.BeginBinaryImportAsync(CopySql, ct))
        {
            // Perform COPY
            foreach (var model in _data)
            {
                await WriteAsync(writer, model, ct);
            }

            await writer.CompleteAsync(ct);
        }

        await conn.CloseAsync();
    }

    private async Task WriteAsync(NpgsqlBinaryImporter writer, Model row, CancellationToken ct)
    {
        await writer.StartRowAsync(ct);
        await writer.WriteAsync(row.key1, NpgsqlDbType.Varchar, ct);
        await writer.WriteAsync(row.key2, NpgsqlDbType.Varchar, ct);
        await writer.WriteAsync(row.col1, NpgsqlDbType.Integer, ct);
        await writer.WriteAsync(row.col2, NpgsqlDbType.Integer, ct);
        await writer.WriteAsync(row.col3, NpgsqlDbType.Varchar, ct);
        await writer.WriteAsync(row.col4, NpgsqlDbType.Timestamp, ct);
        await writer.WriteAsync(row.col5, NpgsqlDbType.Timestamp, ct);
    }

    private PropertyData[] GetProperties()
    {
        var props = typeof(Model).GetProperties();
        return props.Select(x => new PropertyData
        {
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
        public required Func<object?, object?> GetValue { get; init; }
        public NpgsqlDbType DbType { get; init; }
    }

    public class Model
    {
        public required string key1 { get; set; }
        public required string key2 { get; set; }
        public int? col1 { get; set; }
        public required int col2 { get; set; }
        public string? col3 { get; set; }
        public required DateTime col4 { get; set; }
        public DateTime? col5 { get; set; }
    }
}
