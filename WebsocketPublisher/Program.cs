using System.Net.Sockets;
using System.Text;
using System.Diagnostics;
using System.Text.Json;

namespace WebSocketPublisher
{
    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string SERVER_HOST = "localhost";
        private const int SERVER_PORT = 8080;
        private static string logFile = $"tcp-publisher-net8-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("TCP Socket Publisher Starting (.NET 8 Version - simulating WebSocket)...");

            // Wait for user to confirm subscriber is ready
            Console.WriteLine("Make sure TCP Socket Subscriber (.NET 8) is running and ready.");
            Console.WriteLine("Press any key to start the performance test...");
            Console.ReadKey();

            try
            {
                await RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR (.NET 8): {ex.Message}");
                LogMessage($"Stack Trace (.NET 8): {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task RunPerformanceTest()
        {
            using var tcpClient = new TcpClient();

            LogMessage($"Connecting to TCP server at {SERVER_HOST}:{SERVER_PORT} (.NET 8)");

            try
            {
                await tcpClient.ConnectAsync(SERVER_HOST, SERVER_PORT);
                LogMessage("Connected to TCP server (.NET 8)");

                await using var stream = tcpClient.GetStream();

                // Small delay to ensure connection is fully established
                await Task.Delay(1000);

                // Warm-up phase
                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages (.NET 8)");
                await SendMessages(stream, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up phase completed (.NET 8)");

                // Wait a bit before starting actual test
                await Task.Delay(2000);

                // Actual test phase
                LogMessage($"Starting performance test with {TEST_MESSAGES} messages (.NET 8)");
                var stopwatch = Stopwatch.StartNew();

                await SendMessages(stream, TEST_MESSAGES, "TEST");

                stopwatch.Stop();
                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;

                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms (.NET 8)");
                LogMessage($"Throughput: {throughput:F2} messages/second (.NET 8)");

                // Give subscriber time to process all messages
                LogMessage("Waiting for subscriber to process all messages... (.NET 8)");
                await Task.Delay(5000);
            }
            catch (SocketException ex)
            {
                LogMessage($"Socket connection error (.NET 8): {ex.Message}");
                throw;
            }
        }

        private static async Task SendMessages(NetworkStream stream, int messageCount, string phase)
        {
            for (int i = 1; i <= messageCount; i++)
            {
                var message = new
                {
                    Id = i,
                    Phase = phase,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Content = $"Message {i} from TCP Publisher (.NET 8)"
                };

                var json = JsonSerializer.Serialize(message) + "\n"; // Add newline delimiter
                var bytes = Encoding.UTF8.GetBytes(json);

                try
                {
                    await stream.WriteAsync(bytes);
                    await stream.FlushAsync();

                    // Small delay every 100 messages to avoid overwhelming the receiver
                    if (i % 100 == 0)
                    {
                        await Task.Delay(10);
                        LogMessage($"Sent {i} {phase} messages... (.NET 8)");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to send message {i} (.NET 8): {ex.Message}");
                    throw;
                }
            }

            LogMessage($"Completed sending all {messageCount} {phase} messages (.NET 8)");
        }

        private static void LogMessage(string message)
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var logEntry = $"{timestamp} - {message}";

            Console.WriteLine(logEntry);
            File.AppendAllText(logFile, logEntry + Environment.NewLine);
        }
    }
}