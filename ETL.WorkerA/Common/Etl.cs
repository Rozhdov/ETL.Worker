using ETL.WorkerA.Common.Process;

namespace ETL.WorkerA.Common;

public class Etl(IServiceProvider sp)
{
    public async Task ProcessAsync(string key)
    {
        var processor = sp.GetRequiredKeyedService<IProcessor>(key);
        await processor.ProcessAsync();
    }
}