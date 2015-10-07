using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;

namespace TableParallel
{
    internal partial class Program
    {

        private void RunAsync()
        {
            RunBodyAsync().Wait();
        }


        private async Task RunBodyAsync()
        {
            var sw = Stopwatch.StartNew();
            var min = _fromDate.ToUniversalTime().Ticks;
            var max = _toDate.ToUniversalTime().Ticks;

            var table = _tableClient.GetTableReference("WADPerformanceCountersTable");

            var partition = Partitioner.Create(min, max + 1, (max - min)/_parallelism).GetDynamicPartitions();

            var resultBug = new ConcurrentBag<List<WADPerformanceCountersTable>>();

            await partition.ForEachAsync(async tuple =>
            {
                var partitionList = new List<List<WADPerformanceCountersTable>>();

                var query = table.CreateQuery<WADPerformanceCountersTable>()
                    .Where(countersTable =>
                        string.Compare(countersTable.PartitionKey, tuple.Item1.ToString("d19"), StringComparison.Ordinal) >= 0 &&
                        string.Compare(countersTable.PartitionKey, tuple.Item2.ToString("d19"), StringComparison.Ordinal) < 0)
                    .Select(e => e).AsTableQuery();

                TableContinuationToken currentToken = null;

                do
                {
                    var segmented = await query.ExecuteSegmentedAsync(currentToken);
                    currentToken = segmented.ContinuationToken;
                    partitionList.Add(segmented.Results);
                } while (currentToken != null);
                if(_verbose > 0)
                   Console.WriteLine("{0},{1},{2},{3}", Thread.CurrentThread.ManagedThreadId, tuple.Item1, tuple.Item2, partitionList.Sum(list => list.Count));

                resultBug.Add(partitionList.SelectMany(list => list).ToList());
            }, _parallelism);

            sw.Stop();
            var result = resultBug.SelectMany(list => list).ToArray();

            Console.WriteLine("Count:{0}, Min: {1}, Max: {2}, Elapsed: {3:F2} sec, PartitionKey:{4}",
                result.Count(), result.Min(e => e.PartitionKey), result.Max(e => e.PartitionKey), (sw.ElapsedMilliseconds / 1000.0), result.GroupBy(e => e.PartitionKey).Count());
        }
    }

    /// <summary>
    /// http://neue.cc/2014/03/14_448.html
    /// </summary>
    public static class EnumerableExtensions
    {
        public static async Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> action, int concurrency, CancellationToken cancellationToken = default(CancellationToken), bool configureAwait = false)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (action == null) throw new ArgumentNullException(nameof(action));
            if (concurrency <= 0) throw new ArgumentOutOfRangeException("concurrency must be more than 1");

            using (var semaphore = new SemaphoreSlim(concurrency, concurrency))
            {
                var exceptionCount = 0;
                var tasks = new List<Task>();

                foreach (var item in source)
                {
                    if (exceptionCount > 0) break;
                    cancellationToken.ThrowIfCancellationRequested();

                    await semaphore.WaitAsync(cancellationToken).ConfigureAwait(configureAwait);
                    var task = action(item).ContinueWith(t =>
                    {
                        semaphore.Release();

                        if (t.IsFaulted)
                        {
                            Interlocked.Increment(ref exceptionCount);
                            throw t.Exception;
                        }
                    });
                    tasks.Add(task);
                }

                await Task.WhenAll(tasks.ToArray()).ConfigureAwait(configureAwait);
            }
        }
    }
}
