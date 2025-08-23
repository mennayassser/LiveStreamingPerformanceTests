using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace WebSocketConsumerConsole
{
    public class Message
    {
        public int Id { get; set; }
        public string Content { get; set; }
        public long Timestamp { get; set; }
    }

    public class LatencyResult
    {
        public int MessageId { get; set; }
        public double LatencyMs { get; set; }
    }

    class Program
    {
        private const int EXPECTED_TEST_COUNT = 1000000;
        private static readonly List<LatencyResult> Results = new List<LatencyResult>();
        private static int _messagesReceived = 0;
        private static bool _testComplete = false;
        private static string logFile = $"websocket-consumer-{DateTime.Now:yyyyMMdd-HHmmss}.log";
        private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        static async Task Main(string[] args)
        {
            LogMessage("WebSocket Consumer Starting...");
            var serverTask = StartWebSocketServer();
            LogMessage("Server is running. Press any key to stop...");
            Console.ReadKey();
            _testComplete = true;
            await serverTask;
        }

        private static async Task StartWebSocketServer()
        {
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");
            listener.Start();
            LogMessage("Listening on http://localhost:8080/");

            while (!_testComplete)
            {
                try
                {
                    HttpListenerContext context = await listener.GetContextAsync();
                    if (context.Request.IsWebSocketRequest)
                    {
                        HttpListenerWebSocketContext wsContext = await context.AcceptWebSocketAsync(null);
                        WebSocket webSocket = wsContext.WebSocket;
                        _ = Task.Run(() => HandleWebSocketClient(webSocket)); // Fire and forget
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (Exception ex) when (IsExpectedShutdownError(ex))
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogMessage($"Server error: {ex.Message}");
                }
            }
            listener.Stop();
            LogMessage("Server stopped.");
        }

        private static async Task HandleWebSocketClient(WebSocket webSocket)
        {
            byte[] buffer = new byte[4 * 1024]; // 4KB buffer
            StringBuilder sb = new StringBuilder();
            LogMessage("Client connected.");

            try
            {
                while (webSocket.State == WebSocketState.Open && !_testComplete)
                {
                    WebSocketReceiveResult result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "OK", CancellationToken.None);
                        break;
                    }

                    string chunk = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    sb.Append(chunk);

                    if (result.EndOfMessage)
                    {
                        string fullMessage = sb.ToString();
                        sb.Clear();
                        ProcessWebSocketMessage(fullMessage, DateTime.UtcNow.Ticks);
                    }
                }
            }
            catch (Exception ex) when (IsExpectedShutdownError(ex))
            {
            }
            catch (Exception ex)
            {
                LogMessage($"Client handling error: {ex.Message}");
            }
            finally
            {
                webSocket?.Dispose();
                LogMessage("Client disconnected.");
            }
        }

        private static void ProcessWebSocketMessage(string jsonMessage, long receiveTimeTicks)
        {
            try
            {
                Message msg = JsonSerializer.Deserialize<Message>(jsonMessage, _jsonOptions);
                if (msg == null) return;

                if (msg.Content == "ALL_MESSAGES_SENT")
                {
                    LogMessage("Received 'done' signal. Calculating results...");
                    CalculateResults();
                    _testComplete = true;
                    return;
                }

                if (msg.Id > 0) 
                {
                    double latencyMs = (receiveTimeTicks - msg.Timestamp) / TimeSpan.TicksPerMillisecond;
                    lock (Results)
                    {
                        Results.Add(new LatencyResult { MessageId = msg.Id, LatencyMs = latencyMs });
                        _messagesReceived++;

                        if (_messagesReceived % 10000 == 0)
                            LogMessage($"Received {_messagesReceived:N0} messages...");
                    }
                }
            }
            catch (JsonException)
            {
                LogMessage($"Failed to parse message: {jsonMessage}");
            }
            catch (Exception ex)
            {
                LogMessage($"Error processing message: {ex.Message}");
            }
        }

        private static void CalculateResults()
        {
            if (Results.Count == 0)
            {
                LogMessage("No results to calculate.");
                return;
            }

            var latencies = Results.Select(r => r.LatencyMs).OrderBy(l => l).ToArray();
            double min = latencies.First();
            double max = latencies.Last();
            double avg = latencies.Average();
            double p95 = GetPercentile(latencies, 0.95);
            double p99 = GetPercentile(latencies, 0.99);

            LogMessage("===== RESULTS =====");
            LogMessage($"Messages Processed: {Results.Count:N0}");
            LogMessage($"Min Latency: {min:F2} ms");
            LogMessage($"Max Latency: {max:F2} ms");
            LogMessage($"Average Latency: {avg:F2} ms");
            LogMessage($"95th Percentile: {p95:F2} ms");
            LogMessage($"99th Percentile: {p99:F2} ms");
            LogMessage("===================");

            string csvPath = $"websocket-latency-{DateTime.Now:yyyyMMdd-HHmmss}.csv";
            File.WriteAllLines(csvPath, new[] { "MessageId,LatencyMs" }.Concat(Results.OrderBy(r => r.MessageId).Select(r => $"{r.MessageId},{r.LatencyMs}")));
            LogMessage($"Detailed results saved to: {csvPath}");
        }

        private static double GetPercentile(double[] sortedData, double percentile)
        {
            int index = (int)Math.Ceiling(percentile * sortedData.Length) - 1;
            index = Math.Max(0, Math.Min(index, sortedData.Length - 1));
            return sortedData[index];
        }

        private static bool IsExpectedShutdownError(Exception ex)
        {
            return ex is OperationCanceledException || ex is WebSocketException;
        }

        private static void LogMessage(string message)
        {
            string entry = $"{DateTime.Now:HH:mm:ss.fff} - {message}";
            Console.WriteLine(entry);
            File.AppendAllText(logFile, entry + Environment.NewLine);
        }
    }
}