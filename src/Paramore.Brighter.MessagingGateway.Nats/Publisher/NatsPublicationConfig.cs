using System;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsPublicationConfig : Publication
    {
        public NatsPublicationConfig()
        {
        }

        public int TopicFindTimeoutMs { get; set; }
    }
}

