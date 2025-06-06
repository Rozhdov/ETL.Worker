﻿using ETL.WorkerC.Common.Extract;
using ETL.WorkerC.Common.Load;
using ETL.WorkerC.Common.Lock;
using ETL.WorkerC.Common.Transform;
using ETL.WorkerC.Extensions;

namespace ETL.WorkerC.Common.Processor;

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
            var newChangeVersion = changeVersion;
            var sourceEnumerator = extractor.ExtractAsync(changeVersion)
                .Chunk(chunkSize).GetAsyncEnumerator();
            var movedNext = await sourceEnumerator.MoveNextAsync();
            if (!movedNext)
            {
                return;
            }

            do
            {
                // Update lock change version after we loaded data in previous iteration.
                // Do nothing on first iteration. 
                var lockTask = newChangeVersion > changeVersion ?
                    @lock.UpdateLockAsync(key, newChangeVersion) : Task.CompletedTask;

                var eColl = sourceEnumerator.Current;
                newChangeVersion = eColl.Max(changeVersionSelector);
                var lColl = transformer.Transform(eColl);

                var eTask = sourceEnumerator.MoveNextAsync().AsTask();
                var lTask = loader.LoadAsync(lColl);

                // Extract next chunk, load current chunk and update lock for previous chunk in one go. 
                await Task.WhenAll(eTask, lTask, lockTask);
                movedNext = eTask.Result;
            } while (movedNext);

            await @lock.UpdateLockAsync(key, newChangeVersion);
        }
        finally
        {
            await @lock.ReleaseLockAsync(key);
        }
    }
}