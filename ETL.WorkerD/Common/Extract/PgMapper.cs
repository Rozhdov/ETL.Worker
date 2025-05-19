using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace ETL.WorkerD.Common.Extract;

public sealed class PgMapper<TExtract>
{
    public Func<DbDataReader, TExtract> GetMapFunction(DbDataReader dataReader)
    {
        MapFunc ??= Prepare(dataReader);
        return MapFunc;
    }

    private Func<DbDataReader, TExtract>? MapFunc;

    private Func<DbDataReader, TExtract> Prepare(DbDataReader dataReader)
    {
        // Prepare type data, filter out properties not present in reader
        var columns = dataReader.GetColumnSchema();
        var propertyInfos = typeof(TExtract).GetProperties().ToArray();
        var columnProperties = columns.Join(propertyInfos, x => x.ColumnName, x => x.Name, (l, r) => (l.ColumnOrdinal, r));
        var readerType = typeof(DbDataReader);
        NullabilityInfoContext _nullabilityContext = new();

        // Prepare DataReader methods info
        var getInt64 = readerType.GetMethod("GetInt64", [typeof(int)])!;
        var getInt32 = readerType.GetMethod("GetInt32", [typeof(int)])!;
        var getString = readerType.GetMethod("GetString", [typeof(int)])!;
        var getDateTime = readerType.GetMethod("GetDateTime", [typeof(int)])!;
        var isDbNull = readerType.GetMethod("IsDBNull", [typeof(int)])!;

        var readerExp = Expression.Parameter(readerType);
        // var result = new TExtract();
        var resultExp = Expression.Variable(typeof(TExtract));
        var initializeResultExp = Expression.Assign(resultExp, Expression.New(typeof(TExtract).GetConstructor(Type.EmptyTypes)!));
        // result.propA = arg.GetInt64(0);
        // if (!arg.IsDbNull(1))
        //     result.propB = (int?)arg.GetInt32(1);
        // ...
        var assignments = columnProperties.Select(x =>
        {
            var (i, propInfo) = x;
            var nullabilityInfo = _nullabilityContext.Create(propInfo);
            var propType = propInfo.PropertyType;
            var underlyingPropType = Nullable.GetUnderlyingType(propType) ?? propType;
            var indexExp = Expression.Constant(i);
            var resultPropExp = Expression.Property(resultExp, propInfo);

            Expression getCallExp;
            if (underlyingPropType == typeof(long))
            {
                getCallExp = Expression.Call(readerExp, getInt64, indexExp);
            }
            else if (underlyingPropType == typeof(int))
            {
                getCallExp = Expression.Call(readerExp, getInt32, indexExp);
            }
            else if (underlyingPropType == typeof(string))
            {
                getCallExp = Expression.Call(readerExp, getString, indexExp);
            }
            else if (underlyingPropType == typeof(DateTime))
            {
                getCallExp = Expression.Call(readerExp, getDateTime, indexExp);
            }
            else
            {
                throw new InvalidOperationException();
            }

            // There is no implicit conversions: assignment of int to int? requires cast
            if (propType != underlyingPropType)
            {
                getCallExp = Expression.Convert(getCallExp, propType);
            }

            var assignPropExp = Expression.Assign(resultPropExp, getCallExp) as Expression;

            var canBeNull = nullabilityInfo.WriteState is NullabilityState.Nullable;
            if (canBeNull)
            {
                assignPropExp = Expression.IfThen(Expression.IsFalse(Expression.Call(readerExp, isDbNull, indexExp)), assignPropExp);
            }

            return assignPropExp;
        });

        // Combine and return result;
        var block = Expression.Block(variables: [resultExp], expressions: assignments.Prepend(initializeResultExp).Append(resultExp));

        // Compile into Func
        var mapFunc = Expression.Lambda<Func<DbDataReader, TExtract>>(block, readerExp).Compile();
        return mapFunc;
    }
}
