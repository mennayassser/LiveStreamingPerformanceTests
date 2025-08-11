using Microsoft.AspNetCore.SignalR.Client;
using System.Diagnostics;
using System.Text.Json;

namespace SignalRPublisher
{
    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string SIGNALR_URL = "http://localhost:8081/performanceTestHub";
        private static string logFile = $"signalr-publisher-net8-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("SignalR Publisher Starting (.NET 8 Version)...");

            // Wait for user to confirm subscriber is ready
            Console.WriteLine("Make sure SignalR Subscriber (.NET 8) is running and ready.");
            Console.WriteLine("Press any key to start the performance test...");
            Console.ReadKey();

            try
            {
                await RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"ERROR: {ex.Message}");
                LogMessage($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        private static async Task RunPerformanceTest()
        {
            var connection = new HubConnectionBuilder()
                .WithUrl(SIGNALR_URL)
                .WithAutomaticReconnect()
                .Build();

            LogMessage($"Connecting to SignalR hub at {SIGNALR_URL} (.NET 8)");

            try
            {
                await connection.StartAsync();
                LogMessage("Connected to SignalR hub (.NET 8)");

                // Small delay to ensure connection is fully established
                await Task.Delay(1000);

                // Warm-up phase
                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages (.NET 8)");
                await SendMessages(connection, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up phase completed (.NET 8)");

                // Wait a bit before starting actual test
                await Task.Delay(2000);

                // Actual test phase
                LogMessage($"Starting performance test with {TEST_MESSAGES} messages (.NET 8)");
                var stopwatch = Stopwatch.StartNew();

                await SendMessages(connection, TEST_MESSAGES, "TEST");

                stopwatch.Stop();
                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;

                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms (.NET 8)");
                LogMessage($"Throughput: {throughput:F2} messages/second (.NET 8)");

                // Give subscriber time to process all messages
                LogMessage("Waiting for subscriber to process all messages... (.NET 8)");
                await Task.Delay(5000);
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        private static async Task SendMessages(HubConnection connection, int messageCount, string phase)
        {
            for (int i = 1; i <= messageCount; i++)
            {
                var message = new
                {
                    Id = i,
                    Phase = phase,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Content = $"Message {i} from SignalR Publisher (.NET 8)"
                };

                try
                {
                    await connection.InvokeAsync("SendMessage", JsonSerializer.SerializeToElement(message));

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