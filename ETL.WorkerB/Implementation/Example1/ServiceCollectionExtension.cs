using ETL.WorkerB.Extensions;

namespace ETL.WorkerB.Implementation.Example1;

public static class ServiceCollectionExtension
{
    public static IServiceCollection AddExample1(this IServiceCollection sc)
    {
        sc.AddEtlProcess<ExtractModel, LoadModel>("Example1")
            .WithExtractor<Extractor>()
            .WithTransformer<Transformer>()
            .WithLoader<Loader>()
            .WithChangeVersion(x => x.key1)
            .Build();

        return sc;
    }
}
