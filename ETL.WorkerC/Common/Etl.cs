using ETL.WorkerC.Common.Processor;

namespace ETL.WorkerC.Common;

public class Etl(IServiceProvider sp)
{
    public async Task ProcessAsync(string key)
    {
        var processor = sp.GetRequiredKeyedService<IProcessor>(key);
        await processor.ProcessAsync();
    }
}