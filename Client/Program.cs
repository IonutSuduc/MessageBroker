using Common;
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Setăm codarea UTF-8 pentru a afișa corect diacriticele în consolă
            Console.OutputEncoding = Encoding.UTF8;
            Console.InputEncoding = Encoding.UTF8;

            string serverIp = Settings.ServerIp;
            int port = Settings.Port;
            string[] topics = Settings.Topics;

            Console.WriteLine("========================================");
            Console.WriteLine("   SISTEM DE MESAGERIE - CLIENT APP    ");
            Console.WriteLine("========================================");
            Console.WriteLine("1. Vreau să trimit mesaje (Sender)");
            Console.WriteLine("2. Vreau să ascult mesaje (Receiver)");
            Console.Write("Alege opțiunea (1 sau 2): ");

            string roleChoice = Console.ReadLine();

            Console.WriteLine("\nAlege subiectul dorit:");
            for (int i = 0; i < topics.Length; i++)
            {
                Console.WriteLine($"{i + 1}. {topics[i]}");
            }
            Console.Write("Introdu numărul subiectului: ");

            if (int.TryParse(Console.ReadLine(), out int topicIndex) && topicIndex >= 1 && topicIndex <= topics.Length)
            {
                string selectedTopic = topics[topicIndex - 1];

                if (roleChoice == "1")
                {
                    Console.Title = $"SENDER - [{selectedTopic}]";
                    await RunAsSenderAsync(serverIp, port, selectedTopic);
                }
                else if (roleChoice == "2")
                {
                    Console.Title = $"RECEIVER - [{selectedTopic}]";

                    // Întrebăm utilizatorul dacă dorește și încărcarea mesajelor precedente
                    Console.Write("Dorești să încarci și mesajele precedente (istoricul)? (da/nu): ");
                    string historyChoice = Console.ReadLine()?.Trim().ToLower();
                    bool loadHistory = (historyChoice == "da" || historyChoice == "d");

                    await RunAsReceiverAsync(serverIp, port, selectedTopic, loadHistory);
                }
                else
                {
                    Console.WriteLine("Opțiune invalidă!");
                }
            }
            else
            {
                Console.WriteLine("Subiect invalid!");
            }

            Console.WriteLine("\nApasă orice tastă pentru a ieși...");
            Console.ReadKey();
        }

        private static async Task RunAsSenderAsync(string serverIp, int port, string subject)
        {
            try
            {
                using (TcpClient client = new TcpClient())
                {
                    await client.ConnectAsync(serverIp, port);
                    using (NetworkStream stream = client.GetStream())
                    using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
                    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        // Anunțăm brokerul că suntem Sender pentru acest subiect specific
                        await writer.WriteLineAsync($"SENDER_INIT:{subject}");
                        string ackInit = await reader.ReadLineAsync();
                        Console.WriteLine($"[SERVER]: {ackInit}");

                        Console.Write("Introdu ID-ul tău (ex: Sender_Ana): ");
                        string senderId = Console.ReadLine();
                        if (string.IsNullOrWhiteSpace(senderId)) senderId = "Anonymous";

                        Console.WriteLine($"\n[SENDER ACTIV] Poți trimite mesaje pe subiectul '{subject}'. Scrie 'exit' pentru oprire.\n");

                        while (true)
                        {
                            Console.Write("Mesaj de trimis: ");
                            string content = Console.ReadLine();

                            if (content?.ToLower() == "exit") break;
                            if (string.IsNullOrWhiteSpace(content)) continue;

                            var msg = new Message
                            {
                                Subject = subject,
                                SenderId = senderId,
                                Content = content
                            };

                            string jsonString = JsonSerializer.Serialize(msg);
                            await writer.WriteLineAsync(jsonString);

                            string response = await reader.ReadLineAsync();
                            Console.WriteLine($"[RĂSPUNS BROKER]: {response}\n");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EROARE SENDER]: {ex.Message}");
            }
        }

        private static async Task RunAsReceiverAsync(string serverIp, int port, string subject, bool loadHistory)
        {
            try
            {
                using (TcpClient client = new TcpClient())
                {
                    await client.ConnectAsync(serverIp, port);
                    using (NetworkStream stream = client.GetStream())
                    using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
                    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        // Trimitem comanda corespunzătoare în funcție de opțiunea aleasă
                        if (loadHistory)
                        {
                            await writer.WriteLineAsync($"SUBSCRIBE_WITH_HISTORY:{subject}");
                        }
                        else
                        {
                            await writer.WriteLineAsync($"SUBSCRIBE:{subject}");
                        }

                        string response = await reader.ReadLineAsync();
                        Console.WriteLine($"[RĂSPUNS BROKER]: {response}");
                        Console.WriteLine($"\n[RECEIVER] Ascult în timp real pentru subiectul: '{subject}'...\n");

                        while (client.Connected)
                        {
                            string incomingLine = await reader.ReadLineAsync();
                            if (!string.IsNullOrEmpty(incomingLine))
                            {
                                // Încercăm să deserializăm linia primită ca să extragem doar Sender-ul și Conținutul
                                try
                                {
                                    Message msg = JsonSerializer.Deserialize<Message>(incomingLine);
                                    if (msg != null && !string.IsNullOrEmpty(msg.Content))
                                    {
                                        Console.WriteLine($"[{msg.SenderId}]: {msg.Content}");
                                    }
                                    else
                                    {
                                        // Dacă este mesaj de sistem (ex: ACK-uri), îl afișăm direct
                                        Console.WriteLine($"{incomingLine}");
                                    }
                                }
                                catch
                                {
                                    // Fallback dacă primul rând sau un mesaj nu este JSON valid
                                    Console.WriteLine($"{incomingLine}");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EROARE RECEIVER]: {ex.Message}");
            }
        }
    }
}