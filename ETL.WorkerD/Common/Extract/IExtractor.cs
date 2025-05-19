namespace ETL.WorkerD.Common.Extract;

public interface IExtractor<TExtract>
{
    IAsyncEnumerable<TExtract> ExtractAsync(long changeVersion, CancellationToken ct);
}