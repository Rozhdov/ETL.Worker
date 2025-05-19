namespace ETL.WorkerA.Common.Lock;

public interface ILock
{
    bool TryAcquireLock(string key, out long changeVersion);
    void UpdateLock(string key, long changeVersion);
    void ReleaseLock(string key);
}