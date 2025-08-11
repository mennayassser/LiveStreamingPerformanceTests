using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace SignalRSubscriber
{
    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new();
        private const string SIGNALR_URL = "http://localhost:8081";
        private const int expectedTestMessages = 10000;
        private static int receivedTestMessages = 0;
        private static bool testCompleted = false;
        private static bool calculationsComplete = false;
        private static string logFile = $"signalr-subscriber-net8-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("SignalR Subscriber Starting (.NET 8 Version)...");

            try
            {
                await StartSignalRServer();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task StartSignalRServer()
        {
            var builder = Host.CreateDefaultBuilder()
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder
                        .UseUrls(SIGNALR_URL)
                        .ConfigureServices(services =>
                        {
                            services.AddSignalR();
                            services.AddLogging(logging =>
                            {
                                logging.AddConsole();
                                logging.SetMinimumLevel(LogLevel.Information);
                            });
                        })
                        .Configure(app =>
                        {
                            app.UseRouting();
                            app.UseEndpoints(endpoints =>
                            {
                                endpoints.MapHub<PerformanceTestHub>("/performanceTestHub");
                            });
                        });
                });

            var host = builder.Build();

            LogMessage($"SignalR server started on {SIGNALR_URL} (.NET 8)");
            LogMessage("Waiting for messages...");

            // Start the server
            var runTask = host.RunAsync();

            // Wait for test to complete and calculations to finish
            while (!calculationsComplete)
            {
                await Task.Delay(1000);
            }

            await host.StopAsync();
        }

        private static void CalculateAndLogStatistics()
        {
            if (Latencies.Count == 0)
            {
                LogMessage("WARNING: No latency measurements collected");
                calculationsComplete = true;
                return;
            }

            var sortedLatencies = Latencies.Select(l => l.LatencyMs).OrderBy(l => l).ToArray();

            var min = sortedLatencies.First();
            var max = sortedLatencies.Last();
            var mean = sortedLatencies.Average();
            var median = GetPercentile(sortedLatencies, 50);
            var p95 = GetPercentile(sortedLatencies, 95);
            var p99 = GetPercentile(sortedLatencies, 99);

            // Calculate throughput based on first and last message timestamps
            var firstMessage = Latencies.OrderBy(l => l.ReceivedTimestamp).First();
            var lastMessage = Latencies.OrderBy(l => l.ReceivedTimestamp).Last();
            var testDurationSeconds = new TimeSpan(lastMessage.ReceivedTimestamp - firstMessage.ReceivedTimestamp).TotalSeconds;
            var throughput = testDurationSeconds > 0 ? Latencies.Count / testDurationSeconds : 0;

            LogMessage("=== SignalR Performance Results (.NET 8) ===");
            LogMessage($"Total Messages: {Latencies.Count}");
            LogMessage($"Test Duration: {testDurationSeconds:F2} seconds");
            LogMessage($"Throughput: {throughput:F2} messages/second");
            LogMessage("Latency Statistics (ms):");
            LogMessage($"  Min: {min:F3}");
            LogMessage($"  Max: {max:F3}");
            LogMessage($"  Mean: {mean:F3}");
            LogMessage($"  Median: {median:F3}");
            LogMessage($"  95th Percentile: {p95:F3}");
            LogMessage($"  99th Percentile: {p99:F3}");

            // Save detailed results to CSV file
            SaveResultsToCsv();

            calculationsComplete = true;
        }

        private static double GetPercentile(double[] sortedArray, int percentile)
        {
            if (sortedArray.Length == 0) return 0;

            var index = (percentile / 100.0) * (sortedArray.Length - 1);
            var lower = (int)Math.Floor(index);
            var upper = (int)Math.Ceiling(index);

            if (lower == upper)
                return sortedArray[lower];

            var weight = index - lower;
            return sortedArray[lower] * (1 - weight) + sortedArray[upper] * weight;
        }

        private static async Task SaveResultsToCsv()
        {
            var csvPath = $"signalr-results-net8-{DateTime.Now:yyyyMMdd-HHmmss}.csv";

            await using var writer = new StreamWriter(csvPath);
            await writer.WriteLineAsync("MessageId,LatencyMs,SentTimestamp,ReceivedTimestamp");

            foreach (var measurement in Latencies.OrderBy(l => l.MessageId))
            {
                await writer.WriteLineAsync($"{measurement.MessageId},{measurement.LatencyMs:F3},{measurement.SentTimestamp},{measurement.ReceivedTimestamp}");
            }

            LogMessage($"Detailed results saved to: {csvPath}");
        }

        internal static void AddLatencyMeasurement(LatencyMeasurement measurement)
        {
            lock (Latencies)
            {
                Latencies.Add(measurement);
                receivedTestMessages++;

                if (receivedTestMessages % 1000 == 0)
                {
                    LogMessage($"Received {receivedTestMessages} test messages so far... (.NET 8)");
                }

                // Check if we've received all expected messages
                if (receivedTestMessages >= expectedTestMessages && !testCompleted)
                {
                    testCompleted = true;
                    LogMessage("All test messages received. Calculating statistics... (.NET 8)");
                    Task.Run(() => CalculateAndLogStatistics());
                }
            }
        }

        internal static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";

            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }

    public class PerformanceTestHub : Hub
    {
        public async Task SendMessage(JsonElement message)
        {
            try
            {
                var receivedTimestamp = DateTime.UtcNow.Ticks;

                var messageId = message.GetProperty("Id").GetInt32();
                var phase = message.GetProperty("Phase").GetString();
                var sentTimestamp = message.GetProperty("Timestamp").GetInt64();
                var content = message.GetProperty("Content").GetString();

                var latencyTicks = receivedTimestamp - sentTimestamp;
                var latencyMs = new TimeSpan(latencyTicks).TotalMilliseconds;

                // Only collect latencies for TEST phase messages
                if (phase == "TEST")
                {
                    Program.AddLatencyMeasurement(new LatencyMeasurement
                    {
                        MessageId = messageId,
                        LatencyMs = latencyMs,
                        SentTimestamp = sentTimestamp,
                        ReceivedTimestamp = receivedTimestamp
                    });
                }
            }
            catch (Exception ex)
            {
                Program.LogMessage($"Error processing message in hub (.NET 8): {ex.Message}");
            }
        }
    }

    public class LatencyMeasurement
    {
        public int MessageId { get; set; }
        public double LatencyMs { get; set; }
        public long SentTimestamp { get; set; }
        public long ReceivedTimestamp { get; set; }
    }
}