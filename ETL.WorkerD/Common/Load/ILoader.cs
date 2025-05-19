namespace ETL.WorkerD.Common.Load;

public interface ILoader<TLoad>
{
    Task LoadAsync(IReadOnlyCollection<TLoad> collection, CancellationToken ct);
}