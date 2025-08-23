using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace RedpandaConsumer
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long TimestampMs { get; set; }
        public string Content { get; set; }
    }

    public class LatencyMeasurement
    {
        public int MessageId { get; set; }
        public double LatencyMs { get; set; }
        public long ReceivedTimestampMs { get; set; }
        public DateTime ReceivedTime { get; set; }
    }

    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new List<LatencyMeasurement>();
        private const int ExpectedTestMessages = 1000000;
        private static int receivedTestMessages = 0;
        private static int receivedWarmupMessages = 0;
        private static bool testCompleted = false;
        private static string logFile = $"redpanda-consumer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly object lockObject = new object();
        private static Timer metricsTimer;
        private static DateTime? firstWarmupTime = null;
        private static DateTime? lastWarmupTime = null;

        static void Main()
        {
            LogMessage("Consumer Starting...");

            metricsTimer = new Timer(CheckForTestCompletion, null, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2));

            try
            {
                StartKafkaConsumer();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
            }
            finally
            {
                metricsTimer?.Dispose();
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static void StartKafkaConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9093",
                GroupId = "redpanda-perf-test-group",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                FetchMinBytes = 1024,
                FetchWaitMaxMs = 10,
                SocketNagleDisable = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("perf-test");

            LogMessage("Subscribed to topic 'perf-test'");

            var lastProgressLog = DateTime.Now;
            var messagesSinceLastLog = 0;
            var handshakeCompleted = false;

            try
            {
                while (!testCompleted)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (consumeResult == null) continue;

                    var receivedTime = DateTime.Now;
                    var receivedTimestamp = GetCurrentTimestamp();

                    ProcessMessage(consumeResult.Message.Value, receivedTimestamp, receivedTime, ref handshakeCompleted);

                    // Progress logging
                    messagesSinceLastLog++;
                    if ((DateTime.Now - lastProgressLog).TotalSeconds >= 1 || messagesSinceLastLog >= 1000)
                    {
                        if (handshakeCompleted)
                        {
                            LogMessage($"Received {receivedTestMessages:N0}/{ExpectedTestMessages:N0} test messages");
                        }
                        lastProgressLog = DateTime.Now;
                        messagesSinceLastLog = 0;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                LogMessage("Consumer stopped");
            }
            finally
            {
                consumer.Close();
            }
        }

        private static void ProcessMessage(string json, long receivedTimestampMs, DateTime receivedTime, ref bool handshakeCompleted)
        {
            try
            {
                var message = JsonConvert.DeserializeObject<Message>(json);
                if (message == null) return;

                if (message.Phase == "WARMUP")
                {
                    lock (lockObject)
                    {
                        receivedWarmupMessages++;

                        // Track handshake timing
                        if (firstWarmupTime == null)
                        {
                            firstWarmupTime = receivedTime;
                            LogMessage($"Handshake started - first warmup message received at {receivedTime:HH:mm:ss.fff}");
                        }
                        lastWarmupTime = receivedTime;

                        // Log handshake completion
                        if (receivedWarmupMessages >= 1000 && !handshakeCompleted)
                        {
                            handshakeCompleted = true;
                            var handshakeDuration = lastWarmupTime.Value - firstWarmupTime.Value;
                            LogMessage($"Handshake completed: {receivedWarmupMessages} warmup messages in {handshakeDuration.TotalMilliseconds:F0}ms");
                        }
                    }
                    return; // Skip latency measurement for warmup
                }

                if (message.Phase == "TEST")
                {
                    var latencyMs = receivedTimestampMs - message.TimestampMs;

                    lock (lockObject)
                    {
                        Latencies.Add(new LatencyMeasurement
                        {
                            MessageId = message.Id,
                            LatencyMs = latencyMs,
                            ReceivedTimestampMs = receivedTimestampMs,
                            ReceivedTime = receivedTime
                        });

                        receivedTestMessages++;
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to process message: {ex.Message}");
            }
        }

        private static void CheckForTestCompletion(object state)
        {
            lock (lockObject)
            {
                if (receivedTestMessages >= ExpectedTestMessages && !testCompleted)
                {
                    testCompleted = true;
                    LogMessage("Test completed - calculating results...");
                    CalculateAndLogStatistics();
                }
            }
        }

        private static void CalculateAndLogStatistics()
        {
            List<LatencyMeasurement> measurements;
            lock (lockObject)
            {
                measurements = new List<LatencyMeasurement>(Latencies);
            }

            if (measurements.Count == 0)
            {
                LogMessage("No test measurements collected");
                return;
            }

            var latencies = measurements.Select(m => m.LatencyMs).OrderBy(l => l).ToArray();
            var firstMessageTime = measurements.Min(m => m.ReceivedTime);
            var lastMessageTime = measurements.Max(m => m.ReceivedTime);
            var testDuration = lastMessageTime - firstMessageTime;
            var throughput = measurements.Count / testDuration.TotalSeconds;

            LogMessage("=== TEST RESULTS ===");
            LogMessage($"Test Start: {firstMessageTime:yyyy-MM-dd HH:mm:ss.fff}");
            LogMessage($"Test End: {lastMessageTime:yyyy-MM-dd HH:mm:ss.fff}");
            LogMessage($"Duration: {testDuration.TotalMilliseconds:N0}ms");
            LogMessage($"Throughput: {throughput:N0} msg/sec");
            LogMessage($"Messages: {measurements.Count:N0}");
            LogMessage($"Min Latency: {latencies.First():F2}ms");
            LogMessage($"Avg Latency: {latencies.Average():F2}ms");
            LogMessage($"Max Latency: {latencies.Last():F2}ms");
            LogMessage($"P95 Latency: {GetPercentile(latencies, 95):F2}ms");
            LogMessage($"P99 Latency: {GetPercentile(latencies, 99):F2}ms");

            SaveResultsToCsv(measurements);
        }

        private static double GetPercentile(double[] sortedArray, double percentile)
        {
            var index = (percentile / 100.0) * (sortedArray.Length - 1);
            return sortedArray[(int)Math.Round(index)];
        }

        private static void SaveResultsToCsv(List<LatencyMeasurement> measurements)
        {
            var csvPath = $"results-{DateTime.Now:yyyyMMdd-HHmmss}.csv";
            File.WriteAllLines(csvPath,
                new[] { "MessageId,LatencyMs,ReceivedTimestampMs,ReceivedTime" }
                .Concat(measurements.Select(m => $"{m.MessageId},{m.LatencyMs},{m.ReceivedTimestampMs},{m.ReceivedTime:yyyy-MM-dd HH:mm:ss.fff}")));

            LogMessage($"Results saved to {csvPath}");
        }

        private static long GetCurrentTimestamp()
        {
            return (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        }

        private static void LogMessage(string message)
        {
            var logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {message}";
            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }
}