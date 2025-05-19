namespace ETL.WorkerB.Common.Load;

public interface ILoader<TLoad>
{
    Task LoadAsync(IReadOnlyCollection<TLoad> collection);
}