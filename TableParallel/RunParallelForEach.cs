using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using Mono.Options;

namespace TableParallel
{
    internal partial  class Program
    {

        /// <summary>
        /// Use Parallel.ForEach
        /// </summary>
        private void RunParallelForEach()
        {
            var sw = Stopwatch.StartNew();
            var min = _fromDate.ToUniversalTime().Ticks;
            var max = _toDate.ToUniversalTime().Ticks;

            var table = _tableClient.GetTableReference("WADPerformanceCountersTable");

            var partition = Partitioner.Create(min, max + 1, (max - min)/_parallelism).GetDynamicPartitions();
            var resultBug = new ConcurrentBag<List<WADPerformanceCountersTable>>();

            Parallel.ForEach(partition,
                new ParallelOptions {MaxDegreeOfParallelism = _parallelism},
                () => new List<WADPerformanceCountersTable>(),
                (tuple, status, list) =>
                {
                    var resultRange = table.CreateQuery<WADPerformanceCountersTable>()
                        .Where(e =>
                            string.Compare(e.PartitionKey, tuple.Item1.ToString("d19"), StringComparison.Ordinal) >= 0 &&
                            string.Compare(e.PartitionKey, tuple.Item2.ToString("d19"), StringComparison.Ordinal) < 0)
                        .Select(e => e).ToArray();
                    if (_verbose > 0)
                        Console.WriteLine("{0},{1},{2},{3}", Thread.CurrentThread.ManagedThreadId, tuple.Item1, tuple.Item2, resultRange.Count());
                    list.AddRange(resultRange);
                    return list;
                },
                (list) => resultBug.Add(list));

            sw.Stop();

            var result = resultBug.SelectMany(list => list).ToArray();
            Console.WriteLine("Count:{0}, Min: {1}, Max: {2}, Elapsed: {3:F2} sec, PartitionKey:{4}", 
                result.Count(), result.Min(e => e.PartitionKey), result.Max(e => e.PartitionKey), (sw.ElapsedMilliseconds / 1000.0), result.GroupBy(e => e.PartitionKey).Count());
        }
    }
}
