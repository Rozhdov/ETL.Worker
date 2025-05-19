namespace ETL.WorkerA.Common.Extract;

public interface IExtractor<TExtract>
{
    Task<IReadOnlyCollection<TExtract>> ExtractAsync(long changeVersion);
}