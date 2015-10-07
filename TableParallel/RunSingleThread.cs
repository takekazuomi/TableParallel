using System;
using System.Diagnostics;
using System.Linq;

namespace TableParallel
{
    internal partial class Program
    {

        /// <summary>
        /// Single Thread Loop
        /// </summary>
        private void RunSingleThread()
        {
            var sw = Stopwatch.StartNew();
            var min = _fromDate.ToUniversalTime().Ticks;
            var max = _toDate.ToUniversalTime().Ticks;

            var table = _tableClient.GetTableReference("WADPerformanceCountersTable");

            var result = table.CreateQuery<WADPerformanceCountersTable>()
                .Where(e =>
                    string.Compare(e.PartitionKey, min.ToString("d19"), StringComparison.Ordinal) >= 0 &&
                    string.Compare(e.PartitionKey, max.ToString("d19"), StringComparison.Ordinal) < 0)
                .Select(e => e)
                .ToArray();
            sw.Stop();
            Console.WriteLine("Count:{0}, Min: {1}, Max: {2}, Elapsed: {3:F2} sec",
                result.Count(), result.Min(e => e.PartitionKey), result.Max(e => e.PartitionKey), (sw.ElapsedMilliseconds/1000.0));
        }
    }
}
