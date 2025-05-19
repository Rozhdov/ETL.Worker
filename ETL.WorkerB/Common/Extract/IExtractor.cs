namespace ETL.WorkerB.Common.Extract;

public interface IExtractor<TExtract>
{
    IAsyncEnumerable<TExtract> ExtractAsync(long changeVersion);
}