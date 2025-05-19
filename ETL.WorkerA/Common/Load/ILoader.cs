namespace ETL.WorkerA.Common.Load;

public interface ILoader<TLoad>
{
    Task LoadAsync(IReadOnlyCollection<TLoad> collection);
}