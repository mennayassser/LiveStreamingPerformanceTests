using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaHighSpeedProducer
{
    public class Message
    {
        public int Id { get; set; }
        public string Phase { get; set; }
        public long TimestampMs { get; set; }
        public string Content { get; set; }
    }

    class Program
    {
        private const int WARM_UP_MESSAGES = 1000;
        private const int TEST_MESSAGES = 1000000;
        private const string TOPIC_NAME = "perf-test";
        private const int BATCH_FLUSH_SIZE = 10000;
        private static readonly int PRODUCER_THREADS = Environment.ProcessorCount;

        private static readonly string logFile = $"redpanda-producer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly object lockObject = new object();
        private static volatile int totalSent = 0;

        // Pre-allocated message templates for zero-allocation serialization
        private static readonly byte[] warmupMessageBytes;
        private static readonly StringBuilder stringBuilder = new StringBuilder(256);

        static Program()
        {
            // Pre-serialize warmup message to avoid allocation during test
            var warmupMsg = new Message
            {
                Id = 0,
                Phase = "WARMUP",
                TimestampMs = 0,
                Content = "WARMUP"
            };
            warmupMessageBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(warmupMsg));
        }

        static async Task Main()
        {
            // Set process priority and GC settings for maximum performance
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;
            Thread.CurrentThread.Priority = ThreadPriority.Highest;

            // Configure GC for low latency
            System.Runtime.GCSettings.LatencyMode = System.Runtime.GCLatencyMode.SustainedLowLatency;

            LogMessage("Enhanced Producer Starting...");
            Console.WriteLine("Press any key to begin test...");
            Console.ReadKey();

            try
            {
                await RunMultiThreadedPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static async Task RunMultiThreadedPerformanceTest()
        {
            // Create multiple producers for parallel processing
            var producers = new IProducer<Null, byte[]>[PRODUCER_THREADS];
            var tasks = new Task[PRODUCER_THREADS];

            try
            {
                // Initialize producers with optimized configuration
                for (int i = 0; i < PRODUCER_THREADS; i++)
                {
                    producers[i] = new ProducerBuilder<Null, byte[]>(new ProducerConfig
                    {
                        BootstrapServers = "localhost:9093",

                        // Batching optimizations
                        LingerMs = 1,  // Small linger for better batching
                        BatchSize = 1048576,  // 1MB batches
                        BatchNumMessages = 10000,  // More messages per batch

                        // Compression and serialization
                        CompressionType = CompressionType.Lz4,  // Fast compression
                        CompressionLevel = 1,  // Fastest compression level

                        // Acknowledgment and reliability
                        Acks = Acks.Leader,  // Better than None, faster than All
                        EnableIdempotence = false,  // Disable for max speed

                        // Timeouts and retries
                        MessageTimeoutMs = 5000,
                        RequestTimeoutMs = 5000,
                        RetryBackoffMs = 10,
                        MessageSendMaxRetries = 2,

                        // Socket and network optimizations
                        SocketSendBufferBytes = 1048576,  // 1MB send buffer
                        SocketReceiveBufferBytes = 1048576,  // 1MB receive buffer
                        SocketNagleDisable = true,
                        SocketKeepaliveEnable = true,

                        // Queue settings
                        QueueBufferingMaxMessages = 1000000,
                        QueueBufferingMaxKbytes = 2097152,  // 2GB queue

                        // Metadata refresh
                        TopicMetadataRefreshIntervalMs = 10000,

                        // Client settings
                        ApiVersionRequest = true,
                        BrokerVersionFallback = "2.8.0"
                    })
                    .SetErrorHandler((producer, error) =>
                    {
                        LogMessage($"Producer {Thread.CurrentThread.ManagedThreadId} error: {error.Reason}");
                    })
                    .SetLogHandler((producer, logMessage) =>
                    {
                        if (logMessage.Level <= SyslogLevel.Warning)
                        {
                            LogMessage($"Producer log: {logMessage.Message}");
                        }
                    })
                    .Build();
                }

                // Warm-up phase with all producers
                LogMessage($"Starting warmup with {WARM_UP_MESSAGES} messages across {PRODUCER_THREADS} threads");
                var warmupStopwatch = Stopwatch.StartNew();

                for (int i = 0; i < PRODUCER_THREADS; i++)
                {
                    int threadIndex = i;
                    tasks[i] = Task.Run(() => WarmupProducer(producers[threadIndex], threadIndex));
                }

                await Task.WhenAll(tasks);
                warmupStopwatch.Stop();

                // Flush all producers
                await Task.WhenAll(producers.Select(p => Task.Run(() => p.Flush(TimeSpan.FromSeconds(2)))));

                LogMessage($"Warmup completed in {warmupStopwatch.ElapsedMilliseconds}ms");

                // Reset counters
                totalSent = 0;

                // Test phase with parallel producers
                LogMessage($"Starting test with {TEST_MESSAGES:N0} messages across {PRODUCER_THREADS} threads");
                var testStopwatch = Stopwatch.StartNew();

                for (int i = 0; i < PRODUCER_THREADS; i++)
                {
                    int threadIndex = i;
                    tasks[i] = Task.Run(() => TestProducer(producers[threadIndex], threadIndex));
                }

                // Progress monitoring
                var progressTask = Task.Run(() => MonitorProgress(testStopwatch));

                await Task.WhenAll(tasks);
                testStopwatch.Stop();

                // Final flush
                await Task.WhenAll(producers.Select(p => Task.Run(() => p.Flush(TimeSpan.FromSeconds(5)))));

                var elapsedSeconds = testStopwatch.Elapsed.TotalSeconds;
                var throughput = totalSent / elapsedSeconds;

                LogMessage($"=== TEST RESULTS ===");
                LogMessage($"Messages sent: {totalSent:N0}");
                LogMessage($"Test duration: {testStopwatch.ElapsedMilliseconds:N0}ms");
                LogMessage($"Throughput: {throughput:N0} msg/sec");
                LogMessage($"Avg per thread: {throughput / PRODUCER_THREADS:N0} msg/sec");
            }
            finally
            {
                // Dispose all producers
                foreach (var producer in producers)
                {
                    producer?.Dispose();
                }
            }
        }

        private static void WarmupProducer(IProducer<Null, byte[]> producer, int threadIndex)
        {
            var messagesPerThread = WARM_UP_MESSAGES / PRODUCER_THREADS;

            for (int i = 0; i < messagesPerThread; i++)
            {
                producer.Produce(TOPIC_NAME, new Message<Null, byte[]>
                {
                    Value = warmupMessageBytes
                }, deliveryReport => {
                    if (deliveryReport.Error.IsError)
                    {
                        LogMessage($"Warmup delivery error on thread {threadIndex}: {deliveryReport.Error.Reason}");
                    }
                });
            }
        }

        private static void TestProducer(IProducer<Null, byte[]> producer, int threadIndex)
        {
            var messagesPerThread = TEST_MESSAGES / PRODUCER_THREADS;
            var startId = threadIndex * messagesPerThread;
            var localCount = 0;
            var lastFlushTime = Stopwatch.GetTimestamp();

            for (int i = 0; i < messagesPerThread; i++)
            {
                var messageId = startId + i + 1;

                // Fast string building without allocations
                var messageJson = CreateMessageJson(messageId);
                var messageBytes = Encoding.UTF8.GetBytes(messageJson);

                producer.Produce(TOPIC_NAME, new Message<Null, byte[]>
                {
                    Value = messageBytes
                }, deliveryReport => {
                    if (deliveryReport.Error.IsError)
                    {
                        LogMessage($"Delivery error on thread {threadIndex}: {deliveryReport.Error.Reason}");
                    }
                });

                localCount++;
                Interlocked.Increment(ref totalSent);

                // Periodic flush for better performance
                var currentTime = Stopwatch.GetTimestamp();
                if (localCount % BATCH_FLUSH_SIZE == 0 ||
                    (currentTime - lastFlushTime) > Stopwatch.Frequency) // 1 second
                {
                    producer.Poll(TimeSpan.Zero);
                    lastFlushTime = currentTime;
                }
            }
        }

        private static string CreateMessageJson(int messageId)
        {
            // Fast JSON creation without JsonConvert overhead
            var timestamp = (long)(Stopwatch.GetTimestamp() * 1000.0 / Stopwatch.Frequency);

            return $"{{\"Id\":{messageId},\"Phase\":\"TEST\",\"TimestampMs\":{timestamp},\"Content\":\"Message {messageId}\"}}";
        }

        private static async Task MonitorProgress(Stopwatch testStopwatch)
        {
            var lastCount = 0;
            var lastTime = testStopwatch.ElapsedMilliseconds;

            while (testStopwatch.IsRunning && totalSent < TEST_MESSAGES)
            {
                await Task.Delay(2000);

                var currentCount = totalSent;
                var currentTime = testStopwatch.ElapsedMilliseconds;

                if (currentCount > lastCount)
                {
                    var messagesInPeriod = currentCount - lastCount;
                    var timeInPeriod = (currentTime - lastTime) / 1000.0;
                    var currentThroughput = messagesInPeriod / timeInPeriod;

                    LogMessage($"Progress: {currentCount:N0}/{TEST_MESSAGES:N0} messages " +
                              $"({(currentCount * 100.0 / TEST_MESSAGES):F1}%) - " +
                              $"Current: {currentThroughput:N0} msg/sec");

                    lastCount = currentCount;
                    lastTime = currentTime;
                }
            }
        }

        private static void LogMessage(string message)
        {
            var logEntry = $"{DateTime.Now:HH:mm:ss.fff} [{Thread.CurrentThread.ManagedThreadId:D2}] - {message}";
            Console.WriteLine(logEntry);

            lock (lockObject)
            {
                File.AppendAllText(logFile, logEntry + Environment.NewLine);
            }
        }
    }
}

