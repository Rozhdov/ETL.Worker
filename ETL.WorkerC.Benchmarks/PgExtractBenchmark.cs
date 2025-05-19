using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using Dapper;
using Npgsql;
using NpgsqlTypes;
using System.Data.Common;

namespace ETL.WorkerC.Benchmarks;

[MemoryDiagnoser]
[MaxIterationCount(30)]
[EtwProfiler]
public class PgExtractBenchmark
{
    private const string Query = 
        """
        SELECT version, key1, key2, col1, col2, col3, col4, col5
            FROM public.source_table2
            WHERE version > @ChangeVersion
            ORDER BY version
        """;

    private readonly NpgsqlDataSource _dataSource 
        = NpgsqlDataSource.Create("User ID=postgres;Password=postgres;Host=192.168.0.177;Port=5432;");
    private readonly NpgsqlDataSource _dataSource16XBuffer 
        = NpgsqlDataSource.Create("User ID=postgres;Password=postgres;Host=192.168.0.177;Port=5432;Write Buffer Size=131072");

    [Benchmark(Baseline = true)]
    public async Task<List<Model>> ReadDapperNormal()
    {
        var coll = DapperReader(_dataSource);
        List<Model> tst = new();
        await foreach(var val in coll)
        {
            tst.Add(val);
        }
        return tst;
    }

    [Benchmark]
    public async Task<List<Model>> ReadDapper16XBuffer()
    {
        var coll = DapperReader(_dataSource16XBuffer);
        List<Model> tst = new();
        await foreach(var val in coll)
        {
            tst.Add(val);
        }
        return tst;
    }

    [Benchmark]
    public async Task<List<Model>> ReadNormal()
    {
        var coll = Read(_dataSource);
        List<Model> tst = new();
        await foreach(var val in coll)
        {
            tst.Add(val);
        }
        return tst;
    }

    [Benchmark]
    public async Task<List<Model>> Read16XBuffer()
    {
        var coll = Read(_dataSource16XBuffer);
        List<Model> tst = new();
        await foreach(var val in coll)
        {
            tst.Add(val);
        }
        return tst;
    }

    private async IAsyncEnumerable<Model> Read(NpgsqlDataSource dataSource)
    {
        var ct = CancellationToken.None;
        await using var conn = dataSource.CreateConnection();
        await conn.OpenAsync(ct);
        var command = conn.CreateCommand();
        command.CommandText = Query;
        command.Parameters.Add(new NpgsqlParameter("ChangeVersion", NpgsqlDbType.Bigint) { NpgsqlValue = 0 });
        var reader = await command.ExecuteReaderAsync(ct);
        var anyData = await reader.ReadAsync(ct);
        if (!anyData)
        {
            yield break;
        }

        do
        {
            yield return MapRow(reader);
        } while (await reader.ReadAsync(ct));

        await conn.CloseAsync();
    }

    private IAsyncEnumerable<Model> DapperReader(NpgsqlDataSource dataSource)
    {
        var conn = dataSource.CreateConnection();
        return conn.QueryUnbufferedAsync<Model>(Query, new { ChangeVersion = 0 });
    }

    public Model MapRow(DbDataReader reader)
    {
        var model = new Model
        {
            version = reader.GetInt64(0),
            key1 = reader.GetString(1),
            key2 = reader.GetString(2),
            col1 = reader.IsDBNull(3) ? null : (int?)reader.GetInt32(3),
            col2 = reader.GetInt32(4),
            col3 = reader.IsDBNull(5) ? null : reader.GetString(5),
            col4 = reader.GetDateTime(6),
            col5 = reader.IsDBNull(7) ? null : (DateTime?)reader.GetDateTime(7),
        };
        return model;
    }

    public class Model
    {
        public required long version { get; set; }
        public required string key1 { get; set; }
        public required string key2 { get; set; }
        public int? col1 { get; set; }
        public required int col2 { get; set; }
        public string? col3 { get; set; }
        public required DateTime col4 { get; set; }
        public DateTime? col5 { get; set; }
    }
}
