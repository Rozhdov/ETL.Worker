﻿namespace ETL.WorkerC.Common.Transform;

public class NoTransform<TModel> : ITransformer<TModel, TModel>
{
    public IReadOnlyCollection<TModel> Transform(IReadOnlyCollection<TModel> collection)
    {
        return collection;
    }
}