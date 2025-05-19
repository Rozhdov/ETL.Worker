namespace ETL.WorkerB.Common.Processor;

public class ProcessorOptions<TExtract, TLoad>
{
    public required string Key { get; init; }
    public required Func<TExtract, long> ChangeVersionSelector { get; init; }
    public required int ChunkSize { get; init; }
}