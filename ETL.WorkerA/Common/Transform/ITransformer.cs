namespace ETL.WorkerA.Common.Transform;

public interface ITransformer<TExtract, TLoad>
{
    IReadOnlyCollection<TLoad> Transform(IReadOnlyCollection<TExtract> collection);
}