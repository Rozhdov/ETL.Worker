namespace ETL.WorkerC.Common.Load;

public interface ILoader<TLoad>
{
    Task LoadAsync(IReadOnlyCollection<TLoad> collection);
}