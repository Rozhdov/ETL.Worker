using ETL.WorkerD.Builder;
using ETL.WorkerD.Common.Extract;
using ETL.WorkerD.Common.Lock;

namespace ETL.WorkerD.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtl(this IServiceCollection sc)
    {
        sc.AddScoped<ILock, DistributedLock>();
        sc.AddSingleton(typeof(PgMapper<>));
        return sc;
    }
    
    public static ProcessBuilder<TExtract, TLoad> AddEtlProcess<TExtract, TLoad>(this IServiceCollection sc, string key)
    {
        return new ProcessBuilder<TExtract, TLoad>(key, sc);
    }
}