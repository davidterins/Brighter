namespace Paramore.Brighter.MessagingGateway.Nats
{
    /// <inheritdoc />
    /// <summary>
    /// A factory for creating a Nats message consumer from a <see cref="Subscription{T}"/>> 
    /// </summary>
    public class NatsMessageConsumerFactory : IAmAMessageConsumerFactory
    {
        private readonly NatsMessagingGatewayConfiguration _gatewayConfiguration;

        /// <summary>
        /// Initializes a factory with the <see cref="NatsMessagingGatewayConfiguration"/> used to connect to a Nats Broker
        /// </summary>
        /// <param name="gatewayConfiguration">The <see cref="NatsMessagingGatewayConfiguration"/> used to connect to the Broker</param>
        public NatsMessageConsumerFactory(NatsMessagingGatewayConfiguration gatewayConfiguration)
        {
            _gatewayConfiguration = gatewayConfiguration;
        }

        /// <summary>
        /// Creates a consumer from the <see cref="Subscription{T}"/>
        /// </summary>
        /// <param name="subscription">The <see cref="NatsSubscriptionConfig"/> to read</param>
        /// <returns>A consumer that can be used to read from the stream</returns>
        public IAmAMessageConsumer Create(Subscription subscription)
        {
            NatsSubscriptionConfig natsSubscriptionConfig = subscription as NatsSubscriptionConfig;

            if (natsSubscriptionConfig == null)
                throw new ConfigurationException($"We expect a the subscription to be of type {nameof(NatsSubscriptionConfig)}");

            return new NatsMessagePullConsumer(
                gatewayConfiguration: _gatewayConfiguration,
                natsSubscriptionConfig: natsSubscriptionConfig);
        }
    }
}

