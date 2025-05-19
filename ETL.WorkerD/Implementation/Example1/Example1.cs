using ETL.PgLoadGen;
using ETL.WorkerD.Common.Extract;
using ETL.WorkerD.Common.Load;
using ETL.WorkerD.Common.Transform;
using ETL.WorkerD.Extensions;

namespace ETL.WorkerD.Implementation.Example1;

public static class ServiceCollectionExtension
{
    public static IServiceCollection AddExample1(this IServiceCollection sc)
    {
        sc.AddEtlProcess<Model, Model>("Example1")
            .WithExtractor<PgExtractor<Model>>(x =>
            {
                x.Query =
                    """
                    SELECT key1, col1
                    FROM public.source_table1
                    WHERE key1 > @ChangeVersion
                    ORDER BY key1
                    """;
            })
            .WithTransformer<NoTransform<Model>>()
            .WithLoader<PgLoader<Model>>(x =>
            {
                x.Copier = new ModelCopier();
            })
            .WithChangeVersion(x => x.key1)
            .WithChunkSize(500_000)
            .Build();

        return sc;
    }
}

[PgLoadModel("public.target_table1")]
public class Model
{
    [PgLoadKey]
    public required long key1 { get; set; }
    public required string col1 { get; set; }
}