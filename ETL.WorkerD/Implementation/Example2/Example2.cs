using ETL.PgLoadGen;
using ETL.WorkerD.Common.Extract;
using ETL.WorkerD.Common.Load;
using ETL.WorkerD.Common.Transform;
using ETL.WorkerD.Extensions;

namespace ETL.WorkerD.Implementation.Example2;

public static class ServiceCollectionExtension
{
    public static IServiceCollection AddExample2(this IServiceCollection sc)
    {
        sc.AddEtlProcess<ExtractModel, LoadModel>("Example2")
            .WithExtractor<PgExtractor<ExtractModel>>(x =>
            {
                x.Query =
                    """
                    SELECT version, key1, key2, col1, col2, col3, col4, col5
                    FROM public.source_table2
                    WHERE version > @ChangeVersion
                    ORDER BY version
                    """;
            })
            .WithTransformer<Transformer>()
            .WithLoader<PgLoader<LoadModel>>(x =>
            {
                x.Copier = new LoadModelCopier();
            })
            .WithChangeVersion(x => x.version)
            .WithChunkSize(200_000)
            .Build();

        return sc;
    }
}

public class ExtractModel
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

[PgLoadModel("public.target_table2")]
public class LoadModel
{
    [PgLoadKey]
    public required string key1 { get; set; }
    [PgLoadKey]
    public required string key2 { get; set; }
    public int? col1 { get; set; }
    public required int col2 { get; set; }
    public string? col3 { get; set; }
    public required DateTime col4 { get; set; }
    public DateTime? col5 { get; set; }
}

public class Transformer : ITransformer<ExtractModel, LoadModel>
{
    public IReadOnlyCollection<LoadModel> Transform(IReadOnlyCollection<ExtractModel> collection)
    {
        return collection
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
}