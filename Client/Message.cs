namespace Client
{
    public class Message
    {
        public string Subject { get; set; }    // "Prestatii artistice", "Concursuri de recital", "Competitii Sportive"
        public string SenderId { get; set; }
        public string Content { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.Now;
    }
}
