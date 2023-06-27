
namespace Paramore.Brighter.MessagingGateway.Nats
{
    /// <summary>
    /// Abstracts a Nats channel. A channel is a logically addressable pipe.
    /// </summary>
    public class ChannelFactory : IAmAChannelFactory
    {
        private readonly NatsMessageConsumerFactory _natsMessageConsumerFactory;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelFactory"/> class.
        /// </summary>
        /// <param name="NatsMessageConsumerFactory">The messageConsumerFactory.</param>
        public ChannelFactory(NatsMessageConsumerFactory NatsMessageConsumerFactory)
        {
            _natsMessageConsumerFactory = NatsMessageConsumerFactory;
        }

        /// <summary>
        /// Creates the input channel
        /// </summary>
        /// <param name="subscription">The subscription parameters with which to create the channel</param>
        /// <returns></returns>
        public IAmAChannel CreateChannel(Subscription subscription)
        {
            NatsSubscriptionConfig natsSubscription = subscription as NatsSubscriptionConfig;  
            if (natsSubscription == null)
                throw new ConfigurationException("We expect a NatsSubscription or NatsSubscription<T> as a parameter");
            
            return new Channel(
                subscription.ChannelName,
                _natsMessageConsumerFactory.Create(subscription), 
                subscription.BufferSize);
        }
    }
}
