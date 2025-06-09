using ETL.WorkerB.Common.Transform;

namespace ETL.WorkerB.Implementation.Example1;

public class ExampleTransformer : ITransformer<ExtractModel, LoadModel>
{
    public IReadOnlyCollection<LoadModel> Transform(IReadOnlyCollection<ExtractModel> collection)
    {
        return collection.Select(x => new LoadModel{key1 = x.key1, col1 = x.col1 }).ToArray();
    }
}