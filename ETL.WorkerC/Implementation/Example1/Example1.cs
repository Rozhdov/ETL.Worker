using ETL.WorkerC.Common.Extract;
using ETL.WorkerC.Common.Load;
using ETL.WorkerC.Common.Transform;
using ETL.WorkerC.Extensions;

namespace ETL.WorkerC.Implementation.Example1;

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
                x.TableName = "public.target_table1";
            })
            .WithChangeVersion(x => x.key1)
            .WithChunkSize(500_000)
            .Build();

        return sc;
    }
}

public class Model
{
    [KeyColumn]
    public required long key1 { get; set; }
    public required string col1 { get; set; }
}