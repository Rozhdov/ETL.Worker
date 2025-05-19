using ETL.WorkerB.Common.Processor;

namespace ETL.WorkerB.Common;

public class Etl(IServiceProvider sp)
{
    public async Task ProcessAsync(string key)
    {
        var processor = sp.GetRequiredKeyedService<IProcessor>(key);
        await processor.ProcessAsync();
    }
}