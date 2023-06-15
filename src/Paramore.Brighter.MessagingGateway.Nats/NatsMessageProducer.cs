using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    // TODO: Wrap client in here...
    public class NatsMessageProducer : NatsMessagingGateway, IAmAMessageProducer
    {
        IConnection _natsServerConnection = null;

        public NatsMessageProducer(
            NatsMessagingGatewayConfiguration globalConfigartion,
            NatsPublicationConfig natsPublicationConfig)
        {

        }

        /// <summary>
        /// Initialize the producer => two stage construction to allow for a hook if needed
        /// </summary>
        public void Init()
        {
            //_producer = new ProducerBuilder<string, byte[]>(_producerConfig)
            //    .SetErrorHandler((_, error) =>
            //    {
            //        s_logger.LogError("Code: {ErrorCode}, Reason: {ErrorMessage}, Fatal: {FatalError}", error.Code, error.Reason,
            //            error.IsFatal);
            //        _hasFatalProducerError = error.IsFatal;
            //    })
            //    .Build();

            // _publisher = new KafkaMessagePublisher(_producer, _headerBuilder);

            //EnsureTopic();
        }

        private void NatsSamples()
        {
            // Create a new connection factory to create
            // a connection.
            ConnectionFactory cf = new();

            // Creates a live connection to the default
            // NATS Server running locally
            IConnection c = cf.CreateConnection();

            IJetStreamManagement jetStreamManagement = c.CreateJetStreamManagementContext();

            IJetStream jetStreamContext = c.CreateJetStreamContext();

            PublishOptions publishOptions = PublishOptions.Builder().Build();

            var messagePayload = Encoding.UTF8.GetBytes("hej");
            var messageHeader = new MsgHeader();
            var message = new Msg("topic",messageHeader, messagePayload);

            jetStreamContext.Publish(message, publishOptions);

            c.Publish("foo", Encoding.UTF8.GetBytes("hello world"));

            // Publish requests to the given reply subject:
            c.Publish("foo", "bar", Encoding.UTF8.GetBytes("help!"));

            // Draining and closing a connection
            c.Drain();

            // Closing a connection
            c.Close();
        }

        #region IAmMessageProducer

        int IAmAMessageProducer.MaxOutStandingMessages { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        int IAmAMessageProducer.MaxOutStandingCheckIntervalMilliSeconds { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        Dictionary<string, object> IAmAMessageProducer.OutBoxBag { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        #endregion

        #region IDisposable

        void IDisposable.Dispose()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}

