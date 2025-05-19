using System.Collections.Concurrent;

namespace ETL.WorkerA.Common.Lock;

public class ExampleLock : ILock
{
    private readonly ConcurrentDictionary<string, (bool isRunning, long changeVersion)> _lock = new()
    {
        ["Example1"] = (false, 0)
    };
        
    public bool TryAcquireLock(string key, out long changeVersion)
    {
        var state = _lock[key];
        if (state.isRunning)
        {
            changeVersion = default;
            return false;
        }

        var lockAcquired = _lock.TryUpdate(key, state with { isRunning = true }, state);
        if (lockAcquired)
        {
            changeVersion = state.changeVersion;
            return true;
        }
        
        changeVersion = default;
        return false;
    }
    
    public void UpdateLock(string key, long changeVersion)
    {
        _lock[key] = (true, changeVersion);
    }
    
    public void ReleaseLock(string key)
    {
        _lock[key] = _lock[key] with { isRunning = false };
    }
}