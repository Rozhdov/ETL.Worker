using ETL.WorkerB.Builder;
using ETL.WorkerB.Common.Lock;

namespace ETL.WorkerB.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtl(this IServiceCollection sc)
    {
        sc.AddScoped<ILock, DistributedLock>();
        return sc;
    }
    
    public static ProcessBuilder<TExtract, TLoad> AddEtlProcess<TExtract, TLoad>(this IServiceCollection sc, string key)
    {
        return new ProcessBuilder<TExtract, TLoad>(key, sc);
    }
}