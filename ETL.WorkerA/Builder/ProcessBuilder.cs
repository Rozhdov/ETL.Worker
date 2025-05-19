using ETL.WorkerA.Common.Extract;
using ETL.WorkerA.Common.Load;
using ETL.WorkerA.Common.Lock;
using ETL.WorkerA.Common.Process;
using ETL.WorkerA.Common.Transform;

namespace ETL.WorkerA.Builder;

public class ProcessBuilder<TExtract, TLoad>
{
    private readonly string _key;
    private readonly IServiceCollection _serviceCollection;
    private Action<IServiceCollection>? _extractor;
    private Action<IServiceCollection>? _transformer;
    private Action<IServiceCollection>? _loader;
    private Func<TExtract, long>? _changeVersionSelector;

    public ProcessBuilder(string key, IServiceCollection serviceCollection)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(serviceCollection);
        if (serviceCollection.Any(x => x.ServiceKey as string == key))
        {
            throw new ArgumentOutOfRangeException(nameof(key), key, "Process with such name already registered");
        }

        if (serviceCollection.Any(x => x.ServiceType == typeof(IExtractor<TExtract>)))
        {
            throw new ArgumentOutOfRangeException(nameof(key), key, "TExtract type is already used by other process");
        }

        if (serviceCollection.Any(x => x.ServiceType == typeof(ILoader<TLoad>)))
        {
            throw new ArgumentOutOfRangeException(nameof(key), key, "TLoad type is already used by other process");
        }

        _key = key;
        _serviceCollection = serviceCollection;
    }

    public ProcessBuilder<TExtract, TLoad> WithExtractor<TExtractor>()
        where TExtractor : class, IExtractor<TExtract>
    {
        _extractor = sc =>
        {
            sc.AddScoped<IExtractor<TExtract>, TExtractor>();
        };
        return this;
    }

    public ProcessBuilder<TExtract, TLoad> WithTransformer<TTransformer>()
        where TTransformer : class, ITransformer<TExtract, TLoad>
    {
        _transformer = sc =>
        {
            sc.AddScoped<ITransformer<TExtract, TLoad>, TTransformer>();
        };
        return this;
    }

    public ProcessBuilder<TExtract, TLoad> WithLoader<TLoader>()
        where TLoader : class, ILoader<TLoad>
    {
        _loader = sc =>
        {
            sc.AddScoped<ILoader<TLoad>, TLoader>();
        };
        return this;
    }

    public ProcessBuilder<TExtract, TLoad> WithChangeVersion(Func<TExtract, long> changeVersionSelector)
    {
        _changeVersionSelector = changeVersionSelector;
        return this;
    }

    public void Build()
    {
        ArgumentNullException.ThrowIfNull(_extractor);
        ArgumentNullException.ThrowIfNull(_transformer);
        ArgumentNullException.ThrowIfNull(_loader);
        ArgumentNullException.ThrowIfNull(_changeVersionSelector);

        _extractor(_serviceCollection);
        _transformer(_serviceCollection);
        _loader(_serviceCollection);

        _serviceCollection.AddKeyedScoped<IProcessor>(_key, (sp, key) =>
        {
            var extractor = sp.GetRequiredService<IExtractor<TExtract>>();
            var transformer = sp.GetRequiredService<ITransformer<TExtract, TLoad>>();
            var loader = sp.GetRequiredService<ILoader<TLoad>>();
            var @lock = sp.GetRequiredService<ILock>();

            return new Processor<TExtract, TLoad>(_key, _changeVersionSelector,
                extractor, transformer, loader, @lock);
        });
    }
}