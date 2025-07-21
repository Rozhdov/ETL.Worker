using Dapper;
using Npgsql;

namespace ETL.WorkerD.Common.Lock;

public class DistributedLock([FromKeyedServices(ConnectionType.Lock)]NpgsqlConnection conn) : ILock
{
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(10);
    
    public async Task<(bool, long)> TryAcquireLockAsync(string key, Guid executionKey)
    {
        await using var multi = await conn.QueryMultipleAsync(
            """
            UPDATE public.lock_table
            SET execution_key = @executionKey,
                is_running = true,
                lock_expiration = current_timestamp + @LockDuration
            WHERE key = @key 
                AND (is_running = false OR lock_expiration < current_timestamp)
            RETURNING 1;

            SELECT change_version FROM public.lock_table WHERE key = @key;
            """, new { key, LockDuration, executionKey });

        var lockAcquired = await multi.ReadFirstOrDefaultAsync<bool>();
        var changeVersion = await multi.ReadFirstOrDefaultAsync<long>();

        return (lockAcquired, changeVersion);
    } 
    
    public async Task UpdateLockAsync(string key, long changeVersion, Guid executionKey)
    {
        var lockUpdated = await conn.QueryFirstOrDefaultAsync<bool>(
            """
            UPDATE public.lock_table
            SET change_version = @changeVersion
            WHERE key = @key 
                AND execution_key = @executionKey
            RETURNING 1;
            """, new { key, changeVersion, LockDuration, executionKey });

        if (!lockUpdated)
        {
            throw new InvalidOperationException("Cannot update lock. Likely, it is in use by another execution.");
        }
    }
    
    public async Task ReleaseLockAsync(string key, Guid executionKey)
    {
        await conn.ExecuteAsync(
            """
            UPDATE public.lock_table
            SET execution_key = null,
                is_running = false,
                lock_expiration = null
            WHERE key = @key 
                AND execution_key = @executionKey;
            """, new { key, executionKey });
    }
}