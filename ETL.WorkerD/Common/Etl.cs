using ETL.WorkerD.Common.Processor;

namespace ETL.WorkerD.Common;

public class Etl(IServiceProvider sp)
{
    public async Task ProcessAsync(string key, CancellationToken ct)
    {
        var processor = sp.GetRequiredKeyedService<IProcessor>(key);
        await processor.ProcessAsync(ct);
    }
}