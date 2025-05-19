using ETL.WorkerB.Common.Extract;
using ETL.WorkerB.Common.Load;
using ETL.WorkerB.Common.Lock;
using ETL.WorkerB.Common.Transform;
using ETL.WorkerB.Extensions;

namespace ETL.WorkerB.Common.Processor;

public class Processor<TExtract, TLoad>(
    string key,
    Func<TExtract, long> changeVersionSelector,
    int chunkSize,
    IExtractor<TExtract> extractor,
    ITransformer<TExtract, TLoad> transformer,
    ILoader<TLoad> loader,
    ILock @lock)
    : IProcessor
{
    public async Task ProcessAsync()
    {
        var (lockAcquired, changeVersion) = await @lock.TryAcquireLockAsync(key);
        if (!lockAcquired)
        {
            return;
        }

        try
        {
            await foreach (var eColl in extractor.ExtractAsync(changeVersion).Chunk(chunkSize))
            {
                changeVersion = eColl.Max(changeVersionSelector);
                var tColl = transformer.Transform(eColl);
                await loader.LoadAsync(tColl);

                await @lock.UpdateLockAsync(key, changeVersion);
            }
        }
        finally
        {
            await @lock.ReleaseLockAsync(key);
        }
    }
}