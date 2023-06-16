using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.JetStream;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsMessageProducer : NatsMessagingGateway, IAmAMessageProducerSync, IAmAMessageProducerAsync, ISupportPublishConfirmation
    {
        INatsMessageHeaderBuilder _headerBuilder = null;
        NatsMessagePublisher _publisher = null;

        public event Action<bool, Guid> OnMessagePublished;

        public NatsMessageProducer(
            NatsMessagingGatewayConfiguration globalConfiguration,
            NatsPublicationConfig publication)
        {
            if (string.IsNullOrEmpty(publication.Topic))
                throw new ConfigurationException("Topic is required for a publication");

            #region Kafka config
            //------------- Kafka specifics---------------

            //_clientConfig = new ClientConfig
            //{
            //    Acks = (Confluent.Kafka.Acks)((int)publication.Replication),
            //    BootstrapServers = string.Join(",", configuration.BootStrapServers),
            //    ClientId = configuration.Name,
            //    Debug = configuration.Debug,
            //    SaslMechanism = configuration.SaslMechanisms.HasValue ? (Confluent.Kafka.SaslMechanism?)((int)configuration.SaslMechanisms.Value) : null,
            //    SaslKerberosPrincipal = configuration.SaslKerberosPrincipal,
            //    SaslUsername = configuration.SaslUsername,
            //    SaslPassword = configuration.SaslPassword,
            //    SecurityProtocol = configuration.SecurityProtocol.HasValue ? (Confluent.Kafka.SecurityProtocol?)((int)configuration.SecurityProtocol.Value) : null,
            //    SslCaLocation = configuration.SslCaLocation,
            //    SslKeyLocation = configuration.SslKeystoreLocation,
            //};

            //_producerConfig = new ProducerConfig(_clientConfig)
            //{
            //    BatchNumMessages = publication.BatchNumberMessages,
            //    EnableIdempotence = publication.EnableIdempotence,
            //    MaxInFlight = publication.MaxInFlightRequestsPerConnection,
            //    LingerMs = publication.LingerMs,
            //    MessageTimeoutMs = publication.MessageTimeoutMs,
            //    MessageSendMaxRetries = publication.MessageSendMaxRetries,
            //    Partitioner = (Confluent.Kafka.Partitioner)((int)publication.Partitioner),
            //    QueueBufferingMaxMessages = publication.QueueBufferingMaxMessages,
            //    QueueBufferingMaxKbytes = publication.QueueBufferingMaxKbytes,
            //    RequestTimeoutMs = publication.RequestTimeoutMs,
            //    RetryBackoffMs = publication.RetryBackoff,
            //    TransactionalId = publication.TransactionalId,
            //};
            //--------------------------------------
            #endregion

            // Expected properties from inherited Brighter interface
            MakeChannels = publication.MakeChannels;
            Topic = publication.Topic;
            MaxOutStandingMessages = publication.MaxOutStandingMessages;
            MaxOutStandingCheckIntervalMilliSeconds = publication.MaxOutStandingCheckIntervalMilliSeconds;
            OutBoxBag = publication.OutBoxBag;
            TopicFindTimeoutMs = publication.TopicFindTimeoutMs;


            var indexOfStreamNameSplit = Topic.Value.IndexOf(".");

            StreamName = Topic.Value.Substring(0, indexOfStreamNameSplit);
            TopicName = Topic.Value.Substring(indexOfStreamNameSplit + 1);

            //NumPartitions = publication.NumPartitions;
            //ReplicationFactor = publication.ReplicationFactor;

            //_headerBuilder = publication.MessageHeaderBuilder;
        }

        /// <summary>
        /// Initialize the producer => two stage construction to allow for a hook if needed
        /// </summary>
        public void Init()
        {
            _natsServerConnection = new ConnectionFactory().CreateConnection(/*todo: figure out how to create and set options here*/);

            _publisher = new NatsMessagePublisher(_natsServerConnection, StreamName);

            EnsureTopic();
        }

        #region IAmMessageProducer

        /// <summary>
        /// How many outstanding messages may the outbox have before we terminate the programme with an OutboxLimitReached exception?
        /// -1 => No limit, although the Outbox may discard older entries which is implementation dependent
        /// 0 => No outstanding messages, i.e. throw an error as soon as something goes into the Outbox
        /// 1+ => Allow this number of messages to stack up in an Outbox before throwing an exception (likely to fail fast)
        /// </summary>
        public int MaxOutStandingMessages { get; set; } = -1;

        /// <summary>
        /// At what interval should we check the number of outstanding messages has not exceeded the limit set in MaxOutStandingMessages
        /// We spin off a thread to check when inserting an item into the outbox, if the interval since the last insertion is greater than this threshold
        /// If you set MaxOutStandingMessages to -1 or 0 this property is effectively ignored
        /// </summary>
        public int MaxOutStandingCheckIntervalMilliSeconds { get; set; } = 0;

        /// <summary>
        /// An outbox may require additional arguments before it can run its checks. The DynamoDb outbox for example expects there to be a Topic in the args
        /// This bag provides the args required
        /// </summary>
        public Dictionary<string, object> OutBoxBag { get; set; } = new Dictionary<string, object>();

        #endregion

        #region IAmAMessageProducerSync

        void IAmAMessageProducerSync.Send(Message message)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            try
            {
                PublishAck publishAck = _publisher.PublishMessage(message);

                publishAck?.ThrowOnHasError();

                OnMessagePublished?.Invoke(true, message.Header.Id);
            }
            catch (NATSJetStreamException jse)
            {
                throw new ChannelFailureException("Broker communication error.", jse.InnerException);
            }
        }

        void IAmAMessageProducerSync.SendWithDelay(Message message, int delayMilliseconds)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region IAmAMessageProducerAsync

        async Task IAmAMessageProducerAsync.SendAsync(Message message)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            try
            {
                PublishAck publishAck = await _publisher.PublishMessageAsync(message);

                publishAck.ThrowOnHasError();

                OnMessagePublished?.Invoke(true, message.Header.Id);
            }
            catch (NATSJetStreamException jse)
            {
                throw new ChannelFailureException("Broker communication error.", jse.InnerException);
            }
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            _natsServerConnection.Drain();
            _natsServerConnection.Dispose();
        }

        #endregion
    }
}

