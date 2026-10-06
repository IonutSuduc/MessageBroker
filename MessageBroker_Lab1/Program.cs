using Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MessageBrokerApp
{
    class BrokerServer
    {
        private static readonly int Port = Settings.Port;

        private static ConcurrentDictionary<string, ConcurrentQueue<Message>> messageStorage =
            new ConcurrentDictionary<string, ConcurrentQueue<Message>>();

        private static ConcurrentDictionary<string, List<StreamWriter>> subscribers =
            new ConcurrentDictionary<string, List<StreamWriter>>();

        static async Task Main(string[] args)
        {
            // Setăm codarea UTF-8 pentru a afișa corect diacriticele în consolă
            Console.OutputEncoding = Encoding.UTF8;
            Console.InputEncoding = Encoding.UTF8;

            Console.Title = "BROKER - Server Central";

            string[] topics = Settings.Topics;
            foreach (var topic in topics)
            {
                messageStorage[topic] = new ConcurrentQueue<Message>();
                subscribers[topic] = new List<StreamWriter>();
            }

            TcpListener listener = new TcpListener(IPAddress.Any, Port);
            listener.Start();
            Console.WriteLine($"[BROKER] Serverul a pornit și ascultă pe portul {Port}...");

            try
            {
                while (true)
                {
                    TcpClient client = await listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClientAsync(client));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EROARE CRITICĂ BROKER]: {ex.Message}");
            }
            finally
            {
                listener.Stop();
            }
        }

        private static async Task HandleClientAsync(TcpClient client)
        {
            string clientIp = client.Client.RemoteEndPoint?.ToString() ?? "Necunoscut";
            Console.WriteLine($"[REȚEA] Client conectat: {clientIp}");

            try
            {
                using (NetworkStream stream = client.GetStream())
                using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
                {
                    string line = await reader.ReadLineAsync();
                    if (string.IsNullOrEmpty(line)) return;

                    // Gestionare Receiver cu opțiune de încărcare a istoricului
                    if (line.StartsWith("SUBSCRIBE_WITH_HISTORY:") || line.StartsWith("SUBSCRIBE:"))
                    {
                        bool loadHistory = line.StartsWith("SUBSCRIBE_WITH_HISTORY:");
                        string topicToSubscribe = loadHistory
                            ? line.Substring("SUBSCRIBE_WITH_HISTORY:".Length).Trim()
                            : line.Substring("SUBSCRIBE:".Length).Trim();

                        if (subscribers.ContainsKey(topicToSubscribe))
                        {
                            lock (subscribers[topicToSubscribe])
                            {
                                subscribers[topicToSubscribe].Add(writer);
                            }
                            Console.WriteLine($"[SUBSCRIBE] Clientul {clientIp} s-a abonat la subiectul: '{topicToSubscribe}' (Istoric: {loadHistory})");
                            await writer.WriteLineAsync("ACK: Abonare realizată cu succes!");

                            // Dacă s-a cerut istoricul, trimitem mesajele existente din RAM înainte de bucla în timp real
                            if (loadHistory && messageStorage.TryGetValue(topicToSubscribe, out var queue))
                            {
                                foreach (var pastMessage in queue)
                                {
                                    string jsonHistoryMsg = JsonSerializer.Serialize(pastMessage);
                                    await writer.WriteLineAsync(jsonHistoryMsg);
                                }
                            }

                            while (client.Connected)
                            {
                                await Task.Delay(1000);
                            }
                        }
                        else
                        {
                            await writer.WriteLineAsync("ERROR: Subiectul specificat nu există.");
                        }
                        return;
                    }

                    // Gestionare Sender (Poate trimite mai multe mesaje în aceeași sesiune)
                    if (line.StartsWith("SENDER_INIT:"))
                    {
                        string senderTopic = line.Substring("SENDER_INIT:".Length).Trim();
                        Console.WriteLine($"[SENDER] Clientul {clientIp} s-atribuit ca Sender pe subiectul: '{senderTopic}'");
                        await writer.WriteLineAsync("ACK: Pregătit pentru mesaje.");

                        while (client.Connected)
                        {
                            string messageLine = await reader.ReadLineAsync();
                            if (string.IsNullOrEmpty(messageLine)) break;

                            bool isValid = TryParseAndValidateMessage(messageLine, out Message message);
                            if (!isValid || message.Subject != senderTopic)
                            {
                                await writer.WriteLineAsync("ERROR: Mesaj invalid sau subiect necorespunzător.");
                                continue;
                            }

                            messageStorage[message.Subject].Enqueue(message);
                            Console.WriteLine($"[STOCARE] Mesaj nou salvat pentru '{message.Subject}' de la '{message.SenderId}'.");

                            await writer.WriteLineAsync("ACK: Mesaj recepționat și stocat cu succes.");
                            await BroadcastMessageAsync(message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EXCEPȚIE CLIENT]: {ex.Message}");
            }
            finally
            {
                client.Close();
                Console.WriteLine($"[REȚEA] Client deconectat: {clientIp}");
            }
        }

        private static bool TryParseAndValidateMessage(string rawJson, out Message message)
        {
            message = null;
            try
            {
                message = JsonSerializer.Deserialize<Message>(rawJson);
                if (message == null) return false;
                if (string.IsNullOrWhiteSpace(message.Subject)) return false;
                if (string.IsNullOrWhiteSpace(message.Content)) return false;
                if (!messageStorage.ContainsKey(message.Subject)) return false;
                return true;
            }
            catch (JsonException)
            {
                return false;
            }
        }

        private static async Task BroadcastMessageAsync(Message message)
        {
            if (subscribers.TryGetValue(message.Subject, out var list))
            {
                string jsonMessage = JsonSerializer.Serialize(message);
                lock (list)
                {
                    foreach (var subscriberWriter in list)
                    {
                        try
                        {
                            subscriberWriter.WriteLine(jsonMessage);
                        }
                        catch
                        {
                            // Ignorăm erorile de pe conexiuni căzute
                        }
                    }
                }
            }
        }
    }
}