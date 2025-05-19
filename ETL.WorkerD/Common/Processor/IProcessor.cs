namespace ETL.WorkerD.Common.Processor;

public interface IProcessor
{
    Task ProcessAsync(CancellationToken ct);
}