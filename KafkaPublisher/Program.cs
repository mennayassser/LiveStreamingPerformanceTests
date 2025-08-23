using System;
using System.Diagnostics;
using System.IO;
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
        private static readonly string logFile = $"kafka-producer-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static void Main()
        {
            LogMessage("Producer Starting...");
            Console.WriteLine("Press any key to begin test...");
            Console.ReadKey();

            try
            {
                RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static void RunPerformanceTest()
        {
            using var producer = new ProducerBuilder<Null, string>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                LingerMs = 1,
                BatchSize = 1048576,
                CompressionType = CompressionType.Lz4,
                Acks = Acks.Leader,
                // SocketNagleDisable = true,  //groups small network packets together, not needed for this
                MessageTimeoutMs = 5000
            }).Build();

            // Handshake warm-up (no logging of individual messages)
            LogMessage("Starting handshake warmup...");
            var handshakeStartTime = DateTime.Now;

            for (int i = 0; i < WARM_UP_MESSAGES; i++)
            {
                producer.Produce(TOPIC_NAME, new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(new Message
                    {
                        Id = i,
                        Phase = "WARMUP",
                        TimestampMs = GetCurrentTimestamp(),
                        Content = "HANDSHAKE"
                    })
                });
            }

            producer.Flush(TimeSpan.FromSeconds(2));
            var handshakeDuration = DateTime.Now - handshakeStartTime;
            LogMessage($"Handshake completed ({WARM_UP_MESSAGES} messages in {handshakeDuration.TotalMilliseconds:F0}ms)");

            // Test phase
            LogMessage($"Starting test with {TEST_MESSAGES} messages");
            var testStopwatch = Stopwatch.StartNew();
            var lastLogTime = testStopwatch.ElapsedMilliseconds;
            var messagesSinceLastLog = 0;

            for (int i = 1; i <= TEST_MESSAGES; i++)
            {
                producer.Produce(TOPIC_NAME, new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(new Message
                    {
                        Id = i,
                        Phase = "TEST",
                        TimestampMs = GetCurrentTimestamp(),
                        Content = $"Message {i}"
                    })
                });

                // Progress logging
                messagesSinceLastLog++;
                var currentTime = testStopwatch.ElapsedMilliseconds;
                if (currentTime - lastLogTime >= 1000 || messagesSinceLastLog >= 1000)
                {
                    var throughput = (messagesSinceLastLog * 1000.0) / (currentTime - lastLogTime);
                    LogMessage($"Sent {i:N0}/{TEST_MESSAGES:N0} - Current: {throughput:N0} msg/sec");

                    lastLogTime = currentTime;
                    messagesSinceLastLog = 0;
                }
            }

            producer.Flush(TimeSpan.FromSeconds(5));
            testStopwatch.Stop();

            var totalThroughput = TEST_MESSAGES / testStopwatch.Elapsed.TotalSeconds;
            LogMessage($"TEST COMPLETED");
            LogMessage($"Duration: {testStopwatch.ElapsedMilliseconds:N0}ms");
            LogMessage($"Throughput: {totalThroughput:N0} msg/sec");
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