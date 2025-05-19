using ETL.WorkerD.Common.Extract;
using ETL.WorkerD.Common.Load;
using ETL.WorkerD.Common.Lock;
using ETL.WorkerD.Common.Processor;
using ETL.WorkerD.Common.Transform;

namespace ETL.WorkerD.Builder;

public class ProcessBuilder<TExtract, TLoad>
{
    private readonly string _key;
    private readonly IServiceCollection _serviceCollection;
    private Action<IServiceCollection>? _extractor;
    private Action<IServiceCollection>? _transformer;
    private Action<IServiceCollection>? _loader;
    private Func<TExtract, long>? _changeVersionSelector;
    private int _chunkSize = 50_000;

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
    
    public ProcessBuilder<TExtract, TLoad> WithExtractor<TExtractor>(Action<TExtractor> configure)
        where TExtractor : class, IExtractor<TExtract>
    {
        _extractor = sc =>
        {
            sc.AddScoped<TExtractor>();
            sc.AddScoped<IExtractor<TExtract>>(provider =>
            {
                var ext = provider.GetRequiredService<TExtractor>();
                configure(ext);
                return ext;
            });
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
    
    public ProcessBuilder<TExtract, TLoad> WithLoader<TLoader>(Action<TLoader> configure)
        where TLoader : class, ILoader<TLoad>
    {
        _loader = sc =>
        {
            sc.AddScoped<TLoader>();
            sc.AddScoped<ILoader<TLoad>>((provider) =>
            {
                var ext = provider.GetRequiredService<TLoader>();
                configure(ext);
                return ext;
            });
        };
        return this;
    }
    
    public ProcessBuilder<TExtract, TLoad> WithChangeVersion(Func<TExtract, long> changeVersionSelector)
    {
        _changeVersionSelector = changeVersionSelector;
        return this;
    }
    
    public ProcessBuilder<TExtract, TLoad> WithChunkSize(int chunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        _chunkSize = chunkSize;
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

            return new Processor<TExtract, TLoad>(_key, _changeVersionSelector, _chunkSize,
                extractor, transformer, loader, @lock);
        });
    }
}