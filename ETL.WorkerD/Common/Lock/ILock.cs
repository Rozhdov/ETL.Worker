namespace ETL.WorkerD.Common.Lock;

public interface ILock
{
    Task<(bool, long)> TryAcquireLockAsync(string key, Guid executionKey);
    Task UpdateLockAsync(string key, long changeVersion, Guid executionKey);
    Task ReleaseLockAsync(string key, Guid executionKey);
}