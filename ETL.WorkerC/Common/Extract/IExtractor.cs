namespace ETL.WorkerC.Common.Extract;

public interface IExtractor<TExtract>
{
    IAsyncEnumerable<TExtract> ExtractAsync(long changeVersion);
}