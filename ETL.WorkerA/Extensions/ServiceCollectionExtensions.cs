using ETL.WorkerA.Builder;
using ETL.WorkerA.Common.Lock;

namespace ETL.WorkerA.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtl(this IServiceCollection sc)
    {
        sc.AddSingleton<ILock, DistributedLock>();
        return sc;
    }
    
    public static ProcessBuilder<TExtract, TLoad> AddEtlProcess<TExtract, TLoad>(this IServiceCollection sc, string key)
    {
        return new ProcessBuilder<TExtract, TLoad>(key, sc);
    }
}