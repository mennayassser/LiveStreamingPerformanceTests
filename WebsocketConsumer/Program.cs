using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace WebSocketSubscriber
{
    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new();
        private const int expectedTestMessages = 10000;
        private static int receivedTestMessages = 0;
        private static bool testCompleted = false;
        private static string logFile = $"tcp-subscriber-net8-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("TCP Socket Subscriber Starting (.NET 8 Version - simulating WebSocket)...");

            try
            {
                await StartTcpServer();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR (.NET 8): {ex.Message}");
                LogMessage($"Stack Trace (.NET 8): {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task StartTcpServer()
        {
            var listener = new TcpListener(IPAddress.Any, 8080);
            listener.Start();

            LogMessage("TCP server started on port 8080 (.NET 8)");
            LogMessage("Waiting for client connections... (.NET 8)");

            using var cts = new CancellationTokenSource();

            while (!testCompleted && !cts.Token.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await listener.AcceptTcpClientAsync();
                    LogMessage("Client connected (.NET 8)");

                    // Handle connection in background task
                    _ = Task.Run(async () => await HandleClientConnection(tcpClient, cts.Token), cts.Token);
                }
                catch (Exception ex)
                {
                    if (!testCompleted)
                        LogMessage($"Error accepting client connection (.NET 8): {ex.Message}");
                    break;
                }
            }

            listener.Stop();
        }

        private static async Task HandleClientConnection(TcpClient tcpClient, CancellationToken cancellationToken)
        {
            await using var stream = tcpClient.GetStream();

            try
            {
                var buffer = new byte[4096];
                var messageBuffer = new StringBuilder();

                LogMessage("Handling client connection... (.NET 8)");

                while (tcpClient.Connected && !testCompleted && !cancellationToken.IsCancellationRequested)
                {
                    var bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken);
                    if (bytesRead == 0) break;

                    var receivedTimestamp = DateTime.UtcNow.Ticks;
                    var data = Encoding.UTF8.GetString(buffer.AsSpan(0, bytesRead));
                    messageBuffer.Append(data);

                    // Process complete messages (assuming each message ends with \n)
                    string bufferContent = messageBuffer.ToString();
                    string[] messages = bufferContent.Split('\n', StringSplitOptions.RemoveEmptyEntries);

                    // Keep the last incomplete message in buffer
                    if (!bufferContent.EndsWith('\n'))
                    {
                        messageBuffer.Clear();
                        messageBuffer.Append(messages[^1]);
                        messages = messages[..^1];
                    }
                    else
                    {
                        messageBuffer.Clear();
                    }

                    // Process complete messages
                    foreach (var message in messages)
                    {
                        if (!string.IsNullOrWhiteSpace(message))
                        {
                            await ProcessMessage(message.Trim(), receivedTimestamp);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Connection ended (.NET 8): {ex.Message}");
            }
            finally
            {
                tcpClient.Close();
            }
        }

        private static async Task ProcessMessage(string json, long receivedTimestamp)
        {
            try
            {
                var message = JsonSerializer.Deserialize<JsonElement>(json);
                var messageId = message.GetProperty("Id").GetInt32();
                var phase = message.GetProperty("Phase").GetString();
                var sentTimestamp = message.GetProperty("Timestamp").GetInt64();

                var latencyTicks = receivedTimestamp - sentTimestamp;
                var latencyMs = new TimeSpan(latencyTicks).TotalMilliseconds;

                // Only collect latencies for TEST phase messages
                if (phase == "TEST")
                {
                    bool shouldCompleteTest = false;

                    lock (Latencies)
                    {
                        Latencies.Add(new LatencyMeasurement
                        {
                            MessageId = messageId,
                            LatencyMs = latencyMs,
                            SentTimestamp = sentTimestamp,
                            ReceivedTimestamp = receivedTimestamp
                        });

                        receivedTestMessages++;

                        if (receivedTestMessages % 1000 == 0)
                        {
                            LogMessage($"Received {receivedTestMessages} test messages so far... (.NET 8)");
                        }

                        if (receivedTestMessages >= expectedTestMessages)
                        {
                            LogMessage("All test messages received. Calculating statistics... (.NET 8)");
                            shouldCompleteTest = true;
                        }
                    }

                    if (shouldCompleteTest)
                    {
                        await Task.Delay(2000);
                        testCompleted = true;
                        await CalculateAndLogStatistics();
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse message (.NET 8): {ex.Message}");
            }
        }

        private static async Task CalculateAndLogStatistics()
        {
            if (Latencies.Count == 0)
            {
                LogMessage("WARNING: No latency measurements collected (.NET 8)");
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

            LogMessage("=== TCP Socket Performance Results (.NET 8 - WebSocket Simulation) ===");
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
            await SaveResultsToCsv();
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
            var csvPath = $"tcp-socket-results-net8-{DateTime.Now:yyyyMMdd-HHmmss}.csv";

            await using var writer = new StreamWriter(csvPath);
            await writer.WriteLineAsync("MessageId,LatencyMs,SentTimestamp,ReceivedTimestamp");

            foreach (var measurement in Latencies.OrderBy(l => l.MessageId))
            {
                await writer.WriteLineAsync($"{measurement.MessageId},{measurement.LatencyMs:F3},{measurement.SentTimestamp},{measurement.ReceivedTimestamp}");
            }

            LogMessage($"Detailed results saved to: {csvPath}");
        }

        private static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";

            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
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