using ETL.WorkerC.Builder;
using ETL.WorkerC.Common;
using ETL.WorkerC.Common.Lock;

namespace ETL.WorkerC.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtl(this IServiceCollection sc)
    {
        sc.AddScoped<Etl>();
        sc.AddScoped<ILock, DistributedLock>();
        return sc;
    }
    
    public static ProcessBuilder<TExtract, TLoad> AddEtlProcess<TExtract, TLoad>(this IServiceCollection sc, string key)
    {
        return new ProcessBuilder<TExtract, TLoad>(key, sc);
    }
}