using System;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsPublicationConfig : Publication
    {
        public NatsPublicationConfig()
        {
        }

        public string StreamName { get; set; }

        public string Subject { get; set; }

        public int TopicFindTimeoutMs { get; set; }
    }
}

