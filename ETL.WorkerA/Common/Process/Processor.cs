using ETL.WorkerA.Common.Extract;
using ETL.WorkerA.Common.Load;
using ETL.WorkerA.Common.Lock;
using ETL.WorkerA.Common.Transform;

namespace ETL.WorkerA.Common.Process;

public class Processor<TExtract, TLoad>(
    string key,
    Func<TExtract, long> changeVersionSelector,
    IExtractor<TExtract> extractor,
    ITransformer<TExtract, TLoad> transformer,
    ILoader<TLoad> loader,
    ILock @lock)
    : IProcessor
{
    public async Task ProcessAsync()
    {
        if (!@lock.TryAcquireLock(key, out var changeVersion))
        {
            return;
        }

        var eColl = await extractor.ExtractAsync(changeVersion);
        if (eColl.Count == 0)
        {
            return;
        }
        
        changeVersion = eColl.Max(changeVersionSelector);
        var tColl = transformer.Transform(eColl);
        await loader.LoadAsync(tColl);
        
        @lock.UpdateLock(key, changeVersion);
        @lock.ReleaseLock(key);
    }
}