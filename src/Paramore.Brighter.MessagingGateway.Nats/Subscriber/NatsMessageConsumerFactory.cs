namespace Paramore.Brighter.MessagingGateway.Nats
{
    /// <inheritdoc />
    /// <summary>
    /// A factory for creating a Nats message consumer from a <see cref="Subscription{T}"/>> 
    /// </summary>
    public class NatsMessageConsumerFactory : IAmAMessageConsumerFactory
    {
        private readonly NatsMessagingGatewayConfiguration _configuration;

        /// <summary>
        /// Initializes a factory with the <see cref="NatsMessagingGatewayConfiguration"/> used to connect to a Nats Broker
        /// </summary>
        /// <param name="configuration">The <see cref="NatsMessagingGatewayConfiguration"/> used to connect to the Broker</param>
        public NatsMessageConsumerFactory(NatsMessagingGatewayConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <summary>
        /// Creates a consumer from the <see cref="Subscription{T}"/>
        /// </summary>
        /// <param name="subscription">The <see cref="NatsPullSubscription"/> to read</param>
        /// <returns>A consumer that can be used to read from the stream</returns>
        public IAmAMessageConsumer Create(Subscription subscription)
        {
            NatsPullSubscription natsSubscription = subscription as NatsPullSubscription;

            if (natsSubscription == null)
                throw new ConfigurationException($"We expect a the subscription to be of type {nameof(NatsPullSubscription)}");

            return new NatsMessagePullConsumer(
                configuration: _configuration,
                subscriptionName: natsSubscription.Name,
                routingKey: natsSubscription.RoutingKey, //subject
                makeChannels: natsSubscription.MakeChannels
                //groupId: natsSubscription.GroupId,
                //offsetDefault: natsSubscription.OffsetDefault,
                //sessionTimeoutMs: natsSubscription.SessionTimeoutMs,
                //maxPollIntervalMs: natsSubscription.MaxPollIntervalMs,
                //isolationLevel: natsSubscription.IsolationLevel,
                //commitBatchSize: natsSubscription.CommitBatchSize,
                //sweepUncommittedOffsetsIntervalMs: natsSubscription.SweepUncommittedOffsetsIntervalMs,
                //readCommittedOffsetsTimeoutMs: natsSubscription.ReadCommittedOffsetsTimeOutMs,
                //numPartitions: natsSubscription.NumPartitions,
                //partitionAssignmentStrategy: natsSubscription.PartitionAssignmentStrategy,
                //replicationFactor: natsSubscription.ReplicationFactor,
                //topicFindTimeoutMs: natsSubscription.TopicFindTimeoutMs
                );
        }
    }
}

