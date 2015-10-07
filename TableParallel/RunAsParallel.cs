using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace TableParallel
{
    internal partial  class Program
    {

        private void RunAsParallel()
        {
            var sw = Stopwatch.StartNew();
            var min = _fromDate.ToUniversalTime().Ticks;
            var max = _toDate.ToUniversalTime().Ticks;

            var table = _tableClient.GetTableReference("WADPerformanceCountersTable");

            var partition = Partitioner.Create(min, max + 1, (max - min)/_parallelism).GetDynamicPartitions();

            //  The default value is Math.Min(ProcessorCount, MAX_SUPPORTED_DOP) where MAX_SUPPORTED_DOP is 512.
            var result = partition.AsParallel().WithDegreeOfParallelism(_parallelism)
                .Select(tuple =>
                {
                    var resultRange = table.CreateQuery<WADPerformanceCountersTable>()
                        .Where(e =>
                            string.Compare(e.PartitionKey, tuple.Item1.ToString("d19"), StringComparison.Ordinal) >= 0 &&
                            string.Compare(e.PartitionKey, tuple.Item2.ToString("d19"), StringComparison.Ordinal) < 0)
                        .Select(e => e).ToArray();
                    if (_verbose > 0)
                        Console.WriteLine("{0},{1},{2},{3}", Thread.CurrentThread.ManagedThreadId, tuple.Item1, tuple.Item2, resultRange.Count());
                    return resultRange;
                }).ToArray().SelectMany(e => e).ToArray();

            sw.Stop();
            Console.WriteLine("Count:{0}, Min: {1}, Max: {2}, Elapsed: {3:F2} sec, PartitionKey:{4}", 
                result.Count(), result.Min(e => e.PartitionKey), result.Max(e => e.PartitionKey), (sw.ElapsedMilliseconds / 1000.0), result.GroupBy(e => e.PartitionKey).Count());
        }
    }
}
