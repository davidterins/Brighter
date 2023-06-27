using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using NATS.Client;
using NATS.Client.JetStream;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    /// <inheritdoc />
    /// <summary>
    /// Class NatsMessageConsumer is an implementation of <see cref="IAmAMessageConsumer"/>
    /// and provides the facilities to consume messages from a Nats broker for a topic
    /// in a consumer group.
    /// A Nats Message Consumer can create topics, depending on the options chosen.
    /// We store an offset when a message is acknowledged, the message pump does this after successfully invoking a handler.
    /// We commit offsets when the batch size is reached, or the sweeper decides it is too long between commits.
    /// This dual strategy prevents low traffic topics having batches that are 'pending' for long periods, causing a risk that the consumer
    /// will end before committing its offsets.
    /// </summary>
    public class NatsMessagePullConsumer : NatsMessagingGateway, IAmAMessageConsumer
    {
        private readonly NatsMessageCreator _messageCreator;
        private IJetStreamPullSubscription _subscription;
        private bool _isSubscribed = false;
        private bool _disposedValue;

        public NatsMessagePullConsumer(
            NatsMessagingGatewayConfiguration gatewayConfiguration,
            NatsSubscriptionConfig natsSubscriptionConfig)
        {
            if (gatewayConfiguration is null)
            {
                throw new ConfigurationException($"You must set a {nameof(NatsMessagingGatewayConfiguration)} to connect to a broker");
            }

            if (natsSubscriptionConfig is null)
            {
                throw new ConfigurationException($"You must set a {nameof(natsSubscriptionConfig)} for the consumer");
            }

            _messageCreator = new NatsMessageCreator();

            // Set inherited protected members
            MakeChannels = natsSubscriptionConfig.MakeChannels;
            Subject = natsSubscriptionConfig.RoutingKey;
            StreamName = natsSubscriptionConfig.StreamName;
            SubjectFindTimoutMs = natsSubscriptionConfig.SubjectFindTimoutMs;

            Subscribe(natsSubscriptionConfig.RoutingKey.Value, natsSubscriptionConfig.Name.Value);
        }

        /// <summary>
        /// Acknowledges the specified message.
        /// We do not have autocommit on and this stores the message that has just been processed.
        /// We use the header bag to store the partition offset of the message when  reading it from Nats. This enables us to get hold of it when
        /// we acknowledge the message via Brighter. We store the offset via the consumer, and keep an in-memory list of offsets. If we have hit the
        /// batch size we commit the offsets. if not, we trigger the sweeper, which will commit the offset once the specified time interval has passed if
        /// a batch has not done so.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Acknowledge(Message message)
        {
        }

        /// <summary>
        /// There is no 'queue' to purge in Nats, so we treat this as moving past to the offset to the end of any assigned partitions,
        /// thus skipping over anything that exists at that point.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="requeue">if set to <c>true</c> [requeue].</param>
        public void Purge()
        {
        }

        /// <summary>
        /// Receives from the specified topic. Used by a <see cref="Channel"/> to provide access to the stream.
        /// We consume the next offset from the stream, and turn it into a Brighter message; we store the offset in the partition into the Brighter message
        /// headers for use in storing and committing offsets. If the stream is EOF or we are not allocated partitions, returns an empty message. 
        /// </summary>
        /// <param name="timeoutInMilliseconds">The timeout in milliseconds.</param>
        /// <returns>A Brighter message wrapping the payload from the Nats stream</returns>
        // <exception cref="ChannelFailureException">We catch Nats consumer errors and rethrow as a ChannelFailureException </exception>
        public Message[] Receive(int timeoutInMilliseconds)
        {
            try
            {
                //LogOffSets();

                s_logger.LogDebug(
                    "Consuming messages from Nats stream, will wait for {Timeout}", timeoutInMilliseconds);
                try
                {
                    _subscription.Pull(1);
                    Msg consumeResult = _subscription.NextMessage(-1);

                    if (consumeResult == null)
                    {
                        s_logger.LogDebug($"No messages available from Nats stream");
                        return new Message[] { new Message() };
                    }

                    consumeResult.Ack();

                    s_logger.LogDebug("Usable message retrieved from Nats stream: {Request}", Encoding.UTF8.GetString(consumeResult.Data));

                    Message message = _messageCreator.CreateMessage(consumeResult);
                    return new[] { message };

                }
                catch (Exception ex)
                {
                    return new Message[] { new Message() };
                }
            }
        }


        /// <summary>
        /// Rejects the specified message. This is just a commit of the offset to move past the record without processing it
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="requeue">if set to <c>true</c> [requeue].</param>
        public void Reject(Message message)
        {
            //Acknowledge(message);
        }

        /// <summary>
        /// Requeues the specified message. A no-op on Nats as the stream is immutable
        /// </summary>
        /// <param name="message"></param>
        /// <param name="delayMilliseconds">Number of milliseconds to delay delivery of the message.</param>
        /// <returns>False as no requeue support on Nats</returns>
        public bool Requeue(Message message, int delayMilliseconds)
        {
            return false;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _subscription.Dispose();
                    _subscription = null;
                    _natsServerConnection.Drain();
                    _natsServerConnection.Close();
                    _natsServerConnection.Dispose();
                    _natsServerConnection = null;
                }

                _disposedValue = true;
            }
        }

        ~NatsMessagePullConsumer()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Subscribe(string subject, string durableConsumerName)
        {
            s_logger.LogInformation("Nats consumer subscribing to {subject}", subject);

            // TODO: Find somewhere to dispose the nats server connection and also pass in through ctr...
            _natsServerConnection = new ConnectionFactory().CreateConnection();
            IJetStream jetStream = _natsServerConnection.CreateJetStreamContext();

            PullSubscribeOptions pullsubscribeOptions = PullSubscribeOptions.Builder()
                .WithDurable(durableConsumerName)
                .Build();

            _subscription = jetStream.PullSubscribe(Subject.Value, pullsubscribeOptions);

            EnsureSubject();
        }
    }
}

