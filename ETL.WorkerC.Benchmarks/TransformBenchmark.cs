using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using System.Security.Cryptography;

namespace ETL.WorkerC.Benchmarks;

[MemoryDiagnoser]
[MaxIterationCount(30)]
public class TransformBenchmark
{
    private const int Count = 100_000;
    private readonly ExtractModel[] _data;
    private readonly Random _rnd = new();
    
    public TransformBenchmark()
    {
        _data = Enumerable.Range(0, Count).Select(x => new ExtractModel
        {
            version = x,
            key1 = Convert.ToHexString(MD5.HashData(BitConverter.GetBytes(_rnd.Next(1, 1000)))),
            key2 = Convert.ToHexString(MD5.HashData(BitConverter.GetBytes(_rnd.Next(1, 1000)))),
            col1 = _rnd.Next(),
            col2 = _rnd.Next(),
            col3 = string.Concat(Enumerable.Repeat(Guid.NewGuid().ToString(), 10)),
            col4 = DateTime.Now,
            col5 = DateTime.Now
        }).ToArray();
    }

    [Benchmark(Baseline = true)]
    public IReadOnlyCollection<LoadModel> BasicGroupBy()
    {
        return _data
            .GroupBy(x => (x.key1, x.key2))
            .Select(x => x.OrderByDescending(y => y.version).First())
            .Select(x => new LoadModel
            {
                key1 = x.key1,
                key2 = x.key2,
                col1 = x.col1,
                col2 = x.col2,
                col3 = x.col3?.Trim(),
                col4 = x.col4,
                col5 = x.col5
            })
            .ToArray();
    }

    [Benchmark]
    public IReadOnlyCollection<LoadModel> GroupByParallel()
    {
        return _data.AsParallel()
            .GroupBy(x => (x.key1, x.key2))
            .Select(x => x.OrderByDescending(y => y.version).First())
            .Select(x => new LoadModel
            {
                key1 = x.key1,
                key2 = x.key2,
                col1 = x.col1,
                col2 = x.col2,
                col3 = x.col3?.Trim(),
                col4 = x.col4,
                col5 = x.col5
            })
            .ToArray();
    }

    // This one is the fastest and produces correct results, but DistinctBy preserving order is not documented AFAIK,
    // and because of this I'm not comfortable to use it
    [Benchmark]
    public IReadOnlyCollection<LoadModel> OrderDistinct()
    {
        return _data.OrderByDescending(x => x.version)
            .DistinctBy(x => (x.key1, x.key2))
            .Select(x => new LoadModel
            {
                key1 = x.key1,
                key2 = x.key2,
                col1 = x.col1,
                col2 = x.col2,
                col3 = x.col3?.Trim(),
                col4 = x.col4,
                col5 = x.col5
            })
            .ToArray();
    }

    [Benchmark]
    public IReadOnlyCollection<LoadModel> OrderDistinctParallel()
    {
        return _data.AsParallel()
            .OrderByDescending(x => x.version)
            .DistinctBy(x => (x.key1, x.key2))
            .Select(x => new LoadModel
            {
                key1 = x.key1,
                key2 = x.key2,
                col1 = x.col1,
                col2 = x.col2,
                col3 = x.col3?.Trim(),
                col4 = x.col4,
                col5 = x.col5
            })
            .ToArray();
    }

    public class ExtractModel
    {
        public long version { get; set; }
        public required string key1 { get; set; }
        public required string key2 { get; set; }
        public int? col1 { get; set; }
        public required int col2 { get; set; }
        public string? col3 { get; set; }
        public required DateTime col4 { get; set; }
        public DateTime? col5 { get; set; }
    }

    public class LoadModel
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
