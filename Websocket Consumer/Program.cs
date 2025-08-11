using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace WebSocketSubscriber
{
    class Program
    {
        private static readonly List<LatencyMeasurement> Latencies = new List<LatencyMeasurement>();
        private const int expectedTestMessages = 10000;
        private static int receivedTestMessages = 0;
        private static bool testCompleted = false;
        private static string logFile = $"tcp-subscriber-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("TCP Socket Subscriber Starting (simulating WebSocket)...");

            try
            {
                await StartTcpServer();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task StartTcpServer()
        {
            var listener = new TcpListener(IPAddress.Any, 8080);
            listener.Start();

            LogMessage("TCP server started on port 8080");
            LogMessage("Waiting for client connections...");

            while (!testCompleted)
            {
                try
                {
                    var tcpClient = await AcceptTcpClientAsync(listener);
                    LogMessage("Client connected");

                    // Handle connection in background task
                    _ = Task.Run(() => HandleClientConnection(tcpClient));
                }
                catch (Exception ex)
                {
                    if (!testCompleted)
                        LogMessage($"Error accepting client connection: {ex.Message}");
                    break;
                }
            }

            listener.Stop();
        }

        private static async Task<TcpClient> AcceptTcpClientAsync(TcpListener listener)
        {
            return await Task.Factory.FromAsync(listener.BeginAcceptTcpClient, listener.EndAcceptTcpClient, null);
        }

        private static async Task HandleClientConnection(TcpClient tcpClient)
        {
            NetworkStream stream = null;

            try
            {
                stream = tcpClient.GetStream();
                var buffer = new byte[4096];
                var messageBuffer = new StringBuilder();

                LogMessage("Handling client connection...");

                while (tcpClient.Connected && !testCompleted)
                {
                    var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    var receivedTimestamp = DateTime.UtcNow.Ticks;
                    var data = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    messageBuffer.Append(data);

                    // Process complete messages (assuming each message ends with \n)
                    string bufferContent = messageBuffer.ToString();
                    string[] messages = bufferContent.Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);

                    // Keep the last incomplete message in buffer
                    if (!bufferContent.EndsWith("\n"))
                    {
                        messageBuffer.Clear();
                        messageBuffer.Append(messages[messages.Length - 1]);
                        Array.Resize(ref messages, messages.Length - 1);
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
                LogMessage($"Connection ended: {ex.Message}");
            }
            finally
            {
                stream?.Close();
                tcpClient?.Close();
            }
        }

        private static async Task ProcessMessage(string json, long receivedTimestamp)
        {
            try
            {
                var message = JsonConvert.DeserializeObject<dynamic>(json);
                var messageId = (int)message.Id;
                var phase = (string)message.Phase;
                var sentTimestamp = (long)message.Timestamp;

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
                            LogMessage($"Received {receivedTestMessages} test messages so far...");
                        }

                        if (receivedTestMessages >= expectedTestMessages)
                        {
                            LogMessage("All test messages received. Calculating statistics...");
                            shouldCompleteTest = true;
                        }
                    }

                    if (shouldCompleteTest)
                    {
                        await Task.Delay(2000);
                        testCompleted = true;
                        CalculateAndLogStatistics();
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to parse message: {ex.Message}");
            }
        }

        private static void CalculateAndLogStatistics()
        {
            if (Latencies.Count == 0)
            {
                LogMessage("WARNING: No latency measurements collected");
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

            LogMessage("=== TCP Socket Performance Results (WebSocket Simulation) ===");
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

        private static void SaveResultsToCsv()
        {
            var csvPath = $"tcp-socket-results-{DateTime.Now:yyyyMMdd-HHmmss}.csv";

            using (var writer = new StreamWriter(csvPath))
            {
                writer.WriteLine("MessageId,LatencyMs,SentTimestamp,ReceivedTimestamp");

                foreach (var measurement in Latencies.OrderBy(l => l.MessageId))
                {
                    writer.WriteLine($"{measurement.MessageId},{measurement.LatencyMs:F3},{measurement.SentTimestamp},{measurement.ReceivedTimestamp}");
                }
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