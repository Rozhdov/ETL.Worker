using Dapper;
using Npgsql;

namespace ETL.WorkerC.Common.Lock;

public class DistributedLock([FromKeyedServices(ConnectionType.Lock)] NpgsqlConnection conn) : ILock
{
    public async Task<(bool, long)> TryAcquireLockAsync(string key)
    {
        await using var multi = await conn.QueryMultipleAsync(
            """
            UPDATE public.lock_table
            SET is_running = true
            WHERE key = @key AND is_running = false
            RETURNING 1;

            SELECT change_version FROM public.lock_table WHERE key = @key;
            """, new { key });

        var lockAcquired = await multi.ReadFirstOrDefaultAsync<bool>();
        var changeVersion = await multi.ReadFirstOrDefaultAsync<long>();

        return (lockAcquired, changeVersion);
    }
    
    public async Task UpdateLockAsync(string key, long changeVersion)
    {
        await conn.ExecuteAsync(
            """
            UPDATE public.lock_table
            SET change_version = @changeVersion
            WHERE key = @key
            """, new { key, changeVersion });
    }
    
    public async Task ReleaseLockAsync(string key)
    {
        await conn.ExecuteAsync(
            """
            UPDATE public.lock_table
            SET is_running = false
            WHERE key = @key
            """, new { key });
    }
}