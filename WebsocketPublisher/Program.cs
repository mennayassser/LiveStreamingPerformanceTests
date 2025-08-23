using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Text.Json;

namespace WebSocketProducerConsole
{
    public class Message
    {
        public int Id { get; set; }
        public string Content { get; set; }
        public long Timestamp { get; set; }
    }

    class Program
    {
        private const int WARM_UP_MESSAGES = 100;
        private const int TEST_MESSAGES = 1000000;
        private const string SERVER_URI = "ws://localhost:8080/";
        private static string logFile = $"websocket-producer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions { WriteIndented = false };

        static async Task Main(string[] args)
        {
            LogMessage("WebSocket Producer Starting...");
            Console.WriteLine("Press any key to start...");
            Console.ReadKey();

            try
            {
                await RunPerformanceTest();
            }
            catch (Exception ex)
            {
                LogMessage($"FATAL ERROR: {ex}");
            }
            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static async Task RunPerformanceTest()
        {
            using var cts = new CancellationTokenSource();
            using var clientWebSocket = new ClientWebSocket();

            try
            {
                LogMessage($"Connecting to {SERVER_URI}");
                await clientWebSocket.ConnectAsync(new Uri(SERVER_URI), cts.Token);
                LogMessage("Connected.");

                // Warm-up phase
                LogMessage($"Sending {WARM_UP_MESSAGES} warm-up messages...");
                for (int i = 1; i <= WARM_UP_MESSAGES; i++)
                {
                    var msg = new Message { Id = i, Content = $"Warm-up {i}", Timestamp = DateTime.UtcNow.Ticks };
                    await SendMessage(clientWebSocket, msg, cts.Token);
                }
                LogMessage("Warm-up complete.");

                await Task.Delay(2000, cts.Token); // Brief pause

                // Test phase
                LogMessage($"Starting test: {TEST_MESSAGES} messages...");
                var sw = Stopwatch.StartNew();

                for (int i = 1; i <= TEST_MESSAGES; i++)
                {
                    var msg = new Message { Id = i, Content = $"Test {i}", Timestamp = DateTime.UtcNow.Ticks };
                    await SendMessage(clientWebSocket, msg, cts.Token);

                    if (i % 10000 == 0) // Log progress less frequently
                        LogMessage($"Sent {i:N0} messages...");
                }

                sw.Stop();
                LogMessage($"Test phase complete. Elapsed: {sw.Elapsed.TotalSeconds:F2}s. Avg: {TEST_MESSAGES / sw.Elapsed.TotalSeconds:F2} msg/s");

                // Send a final "done" message
                var doneMsg = new Message { Id = -1, Content = "ALL_MESSAGES_SENT", Timestamp = DateTime.UtcNow.Ticks };
                await SendMessage(clientWebSocket, doneMsg, cts.Token);
                LogMessage("'Done' signal sent.");

                // Keep connection open to let consumer finish processing
                await Task.Delay(5000, cts.Token);
                await clientWebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Test Complete", cts.Token);
            }
            catch (Exception ex)
            {
                LogMessage($"Error during test: {ex.Message}");
                cts.Cancel();
            }
        }

        private static async Task SendMessage(ClientWebSocket ws, Message msg, CancellationToken ct)
        {
            string json = JsonSerializer.Serialize(msg, _jsonOptions);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, endOfMessage: true, ct);
        }

        private static void LogMessage(string message)
        {
            string entry = $"{DateTime.Now:HH:mm:ss.fff} - {message}";
            Console.WriteLine(entry);
            File.AppendAllText(logFile, entry + Environment.NewLine);
        }
    }
}