namespace ETL.WorkerD.Common.Lock;

public interface ILock
{
    Task<(bool, long)> TryAcquireLockAsync(string key);
    Task UpdateLockAsync(string key, long changeVersion);
    Task ReleaseLockAsync(string key);
}