using BenchmarkDotNet.Running;
using ETL.WorkerC.Benchmarks;

BenchmarkRunner.Run<PgCopyBenchmark>();
BenchmarkRunner.Run<PgExtractBenchmark>();

Console.ReadKey();