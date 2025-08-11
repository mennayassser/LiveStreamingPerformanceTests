using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using Microsoft.AspNet.SignalR.Client;

namespace SignalRPublisher
{
    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 10000;
        private const string SIGNALR_URL = "http://localhost:8081/signalr";
        private static string logFile = $"signalr-publisher-{DateTime.Now:yyyyMMdd-HHmmss}.log";

        static async Task Main(string[] args)
        {
            LogMessage("SignalR Publisher Starting...");

            // Wait for user to confirm subscriber is ready
            Console.WriteLine("Make sure SignalR Subscriber is running and ready.");
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
            var connection = new HubConnection(SIGNALR_URL);
            var hubProxy = connection.CreateHubProxy("PerformanceTestHub");

            LogMessage($"Connecting to SignalR hub at {SIGNALR_URL}");

            try
            {
                await connection.Start();
                LogMessage("Connected to SignalR hub");

                // Small delay to ensure connection is fully established
                await Task.Delay(1000);

                // Warm-up phase
                LogMessage($"Starting warm-up phase with {WARM_UP_MESSAGES} messages");
                await SendMessages(hubProxy, WARM_UP_MESSAGES, "WARMUP");
                LogMessage("Warm-up phase completed");

                // Wait a bit before starting actual test
                await Task.Delay(2000);

                // Actual test phase
                LogMessage($"Starting performance test with {TEST_MESSAGES} messages");
                var stopwatch = Stopwatch.StartNew();

                await SendMessages(hubProxy, TEST_MESSAGES, "TEST");

                stopwatch.Stop();
                var throughput = TEST_MESSAGES / stopwatch.Elapsed.TotalSeconds;

                LogMessage($"Performance test completed in {stopwatch.ElapsedMilliseconds}ms");
                LogMessage($"Throughput: {throughput:F2} messages/second");

                // Give subscriber time to process all messages
                LogMessage("Waiting for subscriber to process all messages...");
                await Task.Delay(5000);
            }
            finally
            {
                connection.Stop();
                connection.Dispose();
            }
        }

        private static async Task SendMessages(IHubProxy hubProxy, int messageCount, string phase)
        {
            for (int i = 1; i <= messageCount; i++)
            {
                var message = new
                {
                    Id = i,
                    Phase = phase,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Content = $"Message {i} from SignalR Publisher"
                };

                try
                {
                    await hubProxy.Invoke("SendMessage", message);

                    // Small delay every 100 messages to avoid overwhelming the receiver
                    if (i % 100 == 0)
                    {
                        await Task.Delay(10);
                        LogMessage($"Sent {i} {phase} messages...");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to send message {i}: {ex.Message}");
                }
            }

            LogMessage($"Completed sending all {messageCount} {phase} messages");
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