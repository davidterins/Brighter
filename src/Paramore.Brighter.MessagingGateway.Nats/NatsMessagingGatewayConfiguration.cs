namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsMessagingGatewayConfiguration
    {
        /// <summary>
        /// Nats server, defaults to nats://localhost:4222.
        /// </summary>
        public string NatsServer { get; set; } = "nats://localhost:4222";

        /// <summary>
        /// Client identifier.
        /// </summary>
        public string Name { get; set; }

        public string SecurtiyProtocol { get; set; }
    }
}

