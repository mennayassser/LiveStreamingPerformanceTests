using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaPersistentConsumer
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long TimestampMs { get; set; }
        public string Content { get; set; }
    }

    public struct LatencyMeasurement
    {
        public int MessageId;
        public double LatencyMs;
        public long SentTimestampMs;
        public long ReceivedTimestampMs;
    }

    class Program
    {
        private static readonly ConcurrentBag<LatencyMeasurement> Latencies = new ConcurrentBag<LatencyMeasurement>();
        private const int ExpectedTestMessages = 120000;
        private static readonly int CONSUMER_THREADS = Environment.ProcessorCount;
        private const int PROCESSING_BATCH_SIZE = 1000;

        private static volatile int receivedTestMessages = 0;
        private static volatile int processedMessages = 0;
        private static volatile bool testCompleted = false;
        private static volatile bool isConnectedToProducer = false;

        private static readonly string logFile = $"redpanda-consumer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly object lockObject = new object();

        private static long lastMessageTime = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
        private static Timer metricsTimer;
        private static Stopwatch testStopwatch;

        // High-performance message processing pipeline
        private static readonly Channel<(string json, long timestamp)> messageChannel =
            Channel.CreateUnbounded<(string, long)>();

        private static readonly ChannelWriter<(string json, long timestamp)> messageWriter =
            messageChannel.Writer;
        private static readonly ChannelReader<(string json, long timestamp)> messageReader =
            messageChannel.Reader;

        static async Task Main(string[] args)
        {
            // Optimize process for performance
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.SustainedLowLatency;

            LogMessage("Enhanced Kafka Consumer Starting...");
            LogMessage($"Using {CONSUMER_THREADS} consumer threads and {Environment.ProcessorCount} processing threads");

            // Start message processing pipeline
            var processingTasks = new Task[Environment.ProcessorCount];
            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                processingTasks[i] = Task.Run(ProcessMessagesPipeline);
            }

            metricsTimer = new Timer(CheckForProducerDisconnection, null,
                TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

            try
            {
                var consumerTasks = new Task[CONSUMER_THREADS];
                for (int i = 0; i < CONSUMER_THREADS; i++)
                {
                    int consumerId = i;
                    consumerTasks[i] = Task.Run(() => StartKafkaConsumer(consumerId));
                }

                // Monitor progress
                var progressTask = Task.Run(MonitorProgress);

                await Task.WhenAll(consumerTasks);
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }
            finally
            {
                messageWriter.Complete();
                metricsTimer?.Dispose();
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static async Task StartKafkaConsumer(int consumerId)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9093",
                GroupId = "redpanda-perf-test-group",
                ClientId = $"consumer-{consumerId}",

                // Offset management
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 1000,

                // Session and polling
                SessionTimeoutMs = 30000,
                MaxPollIntervalMs = 600000,
                HeartbeatIntervalMs = 3000,

                // Fetch optimization
                FetchMinBytes = 1024,  // Fetch at least 1KB
                FetchWaitMaxMs = 10,   // Low latency fetching
                MaxPartitionFetchBytes = 10485760,  // 10MB per partition
                FetchMaxBytes = 52428800,  // 50MB total fetch

                // Message processing
                EnablePartitionEof = false,

                // Queue settings for high throughput
                QueuedMinMessages = 10000,
                QueuedMaxMessagesKbytes = 65536,  // 64MB queue

                // Socket optimizations
                SocketNagleDisable = true,
                SocketKeepaliveEnable = true,
                SocketSendBufferBytes = 1048576,
                SocketReceiveBufferBytes = 1048576,

                // Advanced settings
                IsolationLevel = IsolationLevel.ReadUncommitted,  // Faster reading
                CheckCrcs = false,  // Skip CRC checks for speed

                // Partition assignment
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) => LogMessage($"Consumer {consumerId} error: {e.Reason}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    LogMessage($"Consumer {consumerId} assigned partitions: [{string.Join(", ", partitions)}]");
                    if (consumerId == 0) // Only log once
                    {
                        LogMessage("Consumers are now ready to receive messages from RedPanda");
                    }
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    LogMessage($"Consumer {consumerId} revoked partitions: [{string.Join(", ", partitions)}]");
                })
                .SetLogHandler((_, logMessage) =>
                {
                    if (logMessage.Level <= SyslogLevel.Warning)
                    {
                        LogMessage($"Consumer {consumerId} log: {logMessage.Message}");
                    }
                })
                .Build();

            consumer.Subscribe("perf-test");
            LogMessage($"Consumer {consumerId} subscribed to RedPanda topic 'perf-test'");

            var messageBuffer = new List<ConsumeResult<Ignore, string>>(PROCESSING_BATCH_SIZE);
            var lastBatchTime = Stopwatch.GetTimestamp();

            try
            {
                while (true)
                {
                    try
                    {
                        // Batch consume for better performance
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1));
                        if (consumeResult != null)
                        {
                            messageBuffer.Add(consumeResult);
                            var currentTime = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);
                            lastMessageTime = currentTime;

                            if (!isConnectedToProducer)
                            {
                                if (consumerId == 0) // Only log once
                                {
                                    LogMessage("Producer connected - resuming message processing");
                                }
                                isConnectedToProducer = true;
                                ResetTestState();
                            }
                        }

                        // Process batch when full or after timeout
                        var timeSinceLastBatch = Stopwatch.GetTimestamp() - lastBatchTime;
                        if (messageBuffer.Count >= PROCESSING_BATCH_SIZE ||
                            (messageBuffer.Count > 0 && timeSinceLastBatch > Stopwatch.Frequency / 1000)) // 1ms
                        {
                            await ProcessMessageBatch(messageBuffer);
                            messageBuffer.Clear();
                            lastBatchTime = Stopwatch.GetTimestamp();
                        }
                    }
                    catch (ConsumeException e)
                    {
                        LogMessage($"Consumer {consumerId} consume error: {e.Error.Reason}");
                        if (e.Error.IsFatal) break;
                    }
                    catch (Exception ex)
                    {
                        LogMessage($"Consumer {consumerId} unexpected error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                LogMessage($"Consumer {consumerId} was cancelled");
            }
            finally
            {
                consumer.Close();
                LogMessage($"Consumer {consumerId} closed");
            }
        }

        private static async Task ProcessMessageBatch(List<ConsumeResult<Ignore, string>> messages)
        {
            var receivedTimestamp = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);

            foreach (var message in messages)
            {
                if (!messageWriter.TryWrite((message.Message.Value, receivedTimestamp)))
                {
                    // Channel is closed, stop processing
                    break;
                }
            }
        }

        private static async Task ProcessMessagesPipeline()
        {
            await foreach (var (json, receivedTimestamp) in messageReader.ReadAllAsync())
            {
                try
                {
                    ProcessSingleMessage(json, receivedTimestamp);
                }
                catch (Exception ex)
                {
                    LogMessage($"Pipeline processing error: {ex.Message}");
                }
            }
        }

        private static void ProcessSingleMessage(string json, long receivedTimestampMs)
        {
            try
            {
                // Fast JSON parsing - consider using System.Text.Json for even better performance
                var message = JsonConvert.DeserializeObject<Message>(json);
                if (message == null) return;

                var latencyMs = receivedTimestampMs - message.TimestampMs;

                if (message.Phase == "TEST")
                {
                    if (testStopwatch == null)
                    {
                        testStopwatch = Stopwatch.StartNew();
                        LogMessage("Test phase started - beginning measurements");
                    }

                    Latencies.Add(new LatencyMeasurement
                    {
                        MessageId = message.Id,
                        LatencyMs = latencyMs,
                        SentTimestampMs = message.TimestampMs,
                        ReceivedTimestampMs = receivedTimestampMs
                    });

                    var received = Interlocked.Increment(ref receivedTestMessages);
                    Interlocked.Increment(ref processedMessages);
                }
                else if (message.Phase == "WARMUP" && message.Id % 100 == 0)
                {
                    LogMessage($"Received warmup message {message.Id}");
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse message: {ex.Message}");
            }
        }

        private static async Task MonitorProgress()
        {
            var lastReceived = 0;
            var lastTime = 0L;
            var startTime = Stopwatch.GetTimestamp();

            while (!testCompleted)
            {
                await Task.Delay(2000);

                var currentReceived = receivedTestMessages;
                var currentTime = Stopwatch.GetTimestamp();

                if (currentReceived > lastReceived)
                {
                    var messagesInPeriod = currentReceived - lastReceived;
                    var timeInPeriod = (currentTime - (lastTime > 0 ? lastTime : startTime)) / (double)Stopwatch.Frequency;
                    var currentThroughput = messagesInPeriod / timeInPeriod;
                    var overallThroughput = currentReceived / ((currentTime - startTime) / (double)Stopwatch.Frequency);

                    LogMessage($"Progress: {currentReceived:N0}/{ExpectedTestMessages:N0} " +
                              $"({(currentReceived * 100.0 / ExpectedTestMessages):F1}%) - " +
                              $"Current: {currentThroughput:N0} msg/sec, Overall: {overallThroughput:N0} msg/sec");

                    lastReceived = currentReceived;
                    lastTime = currentTime;
                }
            }
        }

        private static void CheckForProducerDisconnection(object state)
        {
            if (!isConnectedToProducer) return;

            var timeSinceLastMessage = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency) - lastMessageTime;

            if (timeSinceLastMessage > 5000) // 5 seconds timeout
            {
                LogMessage("Producer appears to be disconnected");
                isConnectedToProducer = false;

                lock (lockObject)
                {
                    if ((receivedTestMessages >= ExpectedTestMessages || receivedTestMessages > 100000) && !testCompleted)
                    {
                        LogMessage("Calculating statistics...");
                        testCompleted = true;
                        testStopwatch?.Stop();
                        Task.Run(() => CalculateAndLogStatistics());
                    }
                    else if (receivedTestMessages > 0)
                    {
                        LogMessage($"Producer disconnected with {receivedTestMessages:N0} messages");
                    }
                }
            }
        }

        private static void ResetTestState()
        {
            lock (lockObject)
            {
                if (testCompleted)
                {
                    LogMessage("Resetting test state");

                    // Clear the concurrent bag by creating a new one
                    while (Latencies.TryTake(out _)) { }

                    receivedTestMessages = 0;
                    processedMessages = 0;
                    testCompleted = false;
                    testStopwatch = null;
                }
            }
        }

        private static void CalculateAndLogStatistics()
        {
            var latencyArray = Latencies.ToArray();

            if (latencyArray.Length == 0)
            {
                LogMessage("No latency measurements available");
                return;
            }

            var sortedLatencies = latencyArray.Select(l => l.LatencyMs).OrderBy(l => l).ToArray();
            var min = sortedLatencies.First();
            var max = sortedLatencies.Last();
            var mean = sortedLatencies.Average();
            var median = GetPercentile(sortedLatencies, 50);
            var p95 = GetPercentile(sortedLatencies, 95);
            var p99 = GetPercentile(sortedLatencies, 99);
            var p999 = GetPercentile(sortedLatencies, 99.9);

            var firstMessage = latencyArray.OrderBy(l => l.ReceivedTimestampMs).First();
            var lastMessage = latencyArray.OrderBy(l => l.ReceivedTimestampMs).Last();
            var testDurationMs = lastMessage.ReceivedTimestampMs - firstMessage.ReceivedTimestampMs;
            var throughput = latencyArray.Length / (testDurationMs / 1000.0);

            // Calculate standard deviation
            var variance = sortedLatencies.Sum(x => Math.Pow(x - mean, 2)) / sortedLatencies.Length;
            var stdDev = Math.Sqrt(variance);

            LogMessage("=== PERFORMANCE RESULTS ===");
            LogMessage($"Total Messages: {latencyArray.Length:N0}");
            LogMessage($"Duration: {testDurationMs:N0}ms ({testDurationMs / 1000.0:F2}s)");
            LogMessage($"Throughput: {throughput:N0} msg/sec");
            LogMessage($"");
            LogMessage($"=== LATENCY STATISTICS ===");
            LogMessage($"Min: {min:F3}ms | Avg: {mean:F3}ms | Max: {max:F3}ms");
            LogMessage($"Std Dev: {stdDev:F3}ms");
            LogMessage($"Percentiles: P50: {median:F3}ms | P95: {p95:F3}ms | P99: {p99:F3}ms | P99.9: {p999:F3}ms");

            SaveResultsToCsv(latencyArray);
            SaveSummaryReport(latencyArray, testDurationMs, throughput, mean, stdDev, median, p95, p99, p999);
        }

        private static double GetPercentile(double[] sortedArray, double percentile)
        {
            var index = (percentile / 100.0) * (sortedArray.Length - 1);
            var lower = (int)Math.Floor(index);
            var upper = (int)Math.Ceiling(index);

            if (lower == upper)
                return sortedArray[lower];

            var weight = index - lower;
            return sortedArray[lower] * (1 - weight) + sortedArray[upper] * weight;
        }

        private static void SaveResultsToCsv(LatencyMeasurement[] measurements)
        {
            var csvPath = $"redpanda-results-{DateTime.Now:yyyyMMdd-HHmmss}.csv";
            using var writer = new StreamWriter(csvPath, false, Encoding.UTF8, 65536);
            writer.WriteLine("MessageId,LatencyMs,SentTimestampMs,ReceivedTimestampMs");

            foreach (var m in measurements.OrderBy(m => m.MessageId))
            {
                writer.WriteLine($"{m.MessageId},{m.LatencyMs:F3},{m.SentTimestampMs},{m.ReceivedTimestampMs}");
            }

            LogMessage($"Detailed results saved to: {csvPath}");
        }

        private static void SaveSummaryReport(LatencyMeasurement[] measurements, double testDurationMs,
            double throughput, double mean, double stdDev, double median, double p95, double p99, double p999)
        {
            var reportPath = $"redpanda-summary-{DateTime.Now:yyyyMMdd-HHmmss}.txt";
            using var writer = new StreamWriter(reportPath);

            writer.WriteLine("=== KAFKA/REDPANDA PERFORMANCE TEST SUMMARY ===");
            writer.WriteLine($"Test Date: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            writer.WriteLine($"Consumer Threads: {CONSUMER_THREADS}");
            writer.WriteLine($"Processing Threads: {Environment.ProcessorCount}");
            writer.WriteLine();

            writer.WriteLine("=== THROUGHPUT METRICS ===");
            writer.WriteLine($"Total Messages: {measurements.Length:N0}");
            writer.WriteLine($"Test Duration: {testDurationMs:N0}ms ({testDurationMs / 1000.0:F2}s)");
            writer.WriteLine($"Throughput: {throughput:N0} messages/second");
            writer.WriteLine($"MB/sec (est.): {(throughput * 200 / 1024 / 1024):F2} MB/s");  // Assuming ~200 bytes per message
            writer.WriteLine();

            writer.WriteLine("=== LATENCY DISTRIBUTION ===");
            writer.WriteLine($"Mean Latency: {mean:F3}ms");
            writer.WriteLine($"Median Latency: {median:F3}ms");
            writer.WriteLine($"Standard Deviation: {stdDev:F3}ms");
            writer.WriteLine($"95th Percentile: {p95:F3}ms");
            writer.WriteLine($"99th Percentile: {p99:F3}ms");
            writer.WriteLine($"99.9th Percentile: {p999:F3}ms");
            writer.WriteLine();

            // Calculate latency buckets for distribution analysis
            var buckets = new Dictionary<string, int>
            {
                ["< 1ms"] = 0,
                ["1-5ms"] = 0,
                ["5-10ms"] = 0,
                ["10-50ms"] = 0,
                ["50-100ms"] = 0,
                ["> 100ms"] = 0
            };

            foreach (var measurement in measurements)
            {
                var latency = measurement.LatencyMs;
                if (latency < 1) buckets["< 1ms"]++;
                else if (latency < 5) buckets["1-5ms"]++;
                else if (latency < 10) buckets["5-10ms"]++;
                else if (latency < 50) buckets["10-50ms"]++;
                else if (latency < 100) buckets["50-100ms"]++;
                else buckets["> 100ms"]++;
            }

            writer.WriteLine("=== LATENCY DISTRIBUTION ===");
            foreach (var bucket in buckets)
            {
                var percentage = (bucket.Value * 100.0) / measurements.Length;
                writer.WriteLine($"{bucket.Key}: {bucket.Value:N0} messages ({percentage:F2}%)");
            }

            LogMessage($"Summary report saved to: {reportPath}");
        }

        private static void LogMessage(string message)
        {
            var logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{Thread.CurrentThread.ManagedThreadId:D2}] - {message}";
            Console.WriteLine(logEntry);

            lock (lockObject)
            {
                File.AppendAllText(logFile, logEntry + Environment.NewLine);
            }
        }
    }
}