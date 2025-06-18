using BenchmarkDotNet.Running;
using ETL.WorkerC.Benchmarks;

BenchmarkRunner.Run<PgCopyBenchmark>();
BenchmarkRunner.Run<PgExtractBenchmark>();
BenchmarkRunner.Run<TransformBenchmark>();

Console.ReadKey();