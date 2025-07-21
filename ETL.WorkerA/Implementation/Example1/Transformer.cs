using ETL.WorkerA.Common.Transform;

namespace ETL.WorkerA.Implementation.Example1;

public class Transformer : ITransformer<ExtractModel, LoadModel>
{
    public IReadOnlyCollection<LoadModel> Transform(IReadOnlyCollection<ExtractModel> collection)
    {
        return collection.Select(x => new LoadModel{key1 = x.key1, col1 = x.col1 }).ToArray();
    }
}