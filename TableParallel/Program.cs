using System;
using System.Linq;
using System.Net;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using Mono.Options;

namespace TableParallel
{
    internal partial  class Program
    {

        private DateTime _fromDate = DateTime.Today;
        private DateTime _toDate = DateTime.Today.AddDays(1);
        private bool _help;
        private OptionSet _optionSet;
        private int _parallelism = Environment.ProcessorCount;
        private int _verbose;

        private readonly CloudTableClient _tableClient;

        public static CloudStorageAccount GetStorageAccount()
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ?? CloudConfigurationManager.GetSetting("StorageConnectionString");

            return CloudStorageAccount.Parse(connectionString);
        }

        public Program()
        {
            var storageAccount = GetStorageAccount();

            _tableClient = storageAccount.CreateCloudTableClient();

            _tableClient.DefaultRequestOptions = new TableRequestOptions
            {
                PayloadFormat = TablePayloadFormat.JsonNoMetadata,
                ServerTimeout = new TimeSpan(0, 100, 0),
                MaximumExecutionTime = new TimeSpan(4, 0, 0),
                RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 1), 60)
            };
        }

        public CloudTable GetTableReference(string tableName)
        {
            return _tableClient.GetTableReference(tableName);
        }

        private string ParseOption(string[] args)
        {
            _optionSet = new OptionSet()
            {
                { "f|from=",        "Requred {FROM} is logs local time for download, -f 2015-09-25T08:00:00", v => _fromDate = DateTime.Parse(v)},
                { "t|to=",          "Requred {TO} is logs local time (not include), -t 2015-09-26T08:00:00", v => _toDate = DateTime.Parse(v)},
                { "p|parallelism=", "{PARA} default is ProcessorCount", v => _parallelism = v==null? Environment.ProcessorCount : int.Parse(v)},
                { "v|verbose",      "verbose output", v => _verbose = v==null ? _verbose : ++_verbose},
                { "h|help",         "this message", v => { _help = v != null; }}
            };

            try
            {
                return _optionSet.Parse(args).FirstOrDefault() ?? "";
            }
            catch (OptionException)
            {
                Console.WriteLine("incorect arguments");
            }
            finally
            {
                Console.WriteLine("fromDate:{0:s}, toDate:{1:s}", _fromDate, _toDate);
            }
            return "";
        }

        private static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = int.MaxValue;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;

            var program = new Program();
            var commands = new[]
            {
                Tuple.Create<string, Action>("RunAsParallel", program.RunAsParallel),
                Tuple.Create<string, Action>("RunAsync", program.RunAsync),
                Tuple.Create<string, Action>("RunSingleThread", program.RunSingleThread),
                Tuple.Create<string, Action>("RunParallelForEach", program.RunParallelForEach)
  
            }.ToDictionary(tuple => tuple.Item1);

            var command = program.ParseOption(args);
            if(program._help)
                ShowHelp(program._optionSet, commands.Keys.ToArray());

            if (program._verbose > 1)
                OperationContext.GlobalRequestCompleted += (sender, args2) =>
                {
                    Console.WriteLine("Start:{0}, Elapsed:{1:F2}, {2}, {3}, {4}",
                        args2.RequestInformation.StartTime,
                        (args2.RequestInformation.EndTime - args2.RequestInformation.StartTime).TotalSeconds,
                        args2.Request.Method,
                        args2.Request.RequestUri,
                        args2.RequestInformation.HttpStatusCode);
                };

            Tuple<string, Action> action;
            if (commands.TryGetValue(command, out action))
            {
                action.Item2();
            }
            else
            {
                ShowHelp(program._optionSet, commands.Keys.ToArray());
            }
        }
        static void ShowHelp(OptionSet p, string[] commands)
        {
            Console.WriteLine("Usage: TableParallel [OPTIONS]+ COMMAND ");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
            Console.WriteLine();
            Console.WriteLine("Commands:");
            Console.WriteLine("  {0}", string.Join("|", commands));
        }
    }
}
