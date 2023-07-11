using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.JetStream;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    internal class NatsMessagePublisher
    {
        private readonly INatsMessageHeaderBuilder _headerBuilder;
        private readonly IConnection _natsServerConnection;

        public NatsMessagePublisher(
            IConnection natsServerConnection
            /*INatsMessageHeaderBuilder headerBuilder <---- ignore headers for now...*/)
        {
            _natsServerConnection = natsServerConnection;
        }

        public PublishAck PublishMessage(Message message)
        {
            Msg natsMessage = BuildNatsMessage(message);

            IJetStream jetStream = _natsServerConnection.CreateJetStreamContext();

            PublishOptions publishOptions = PublishOptions.Builder()
                .WithMessageId(message.Id.ToString())
                .Build();

            return jetStream.Publish(natsMessage, publishOptions);
        }

        public async Task<PublishAck> PublishMessageAsync(Message message)
        {
            Msg natsMessage = BuildNatsMessage(message);

            IJetStream jetStream = _natsServerConnection.CreateJetStreamContext();

            PublishOptions publishOptions = PublishOptions.Builder()
                .WithMessageId(message.Id.ToString())
                .Build();

            return await jetStream.PublishAsync(natsMessage, publishOptions);
        }

        private Msg BuildNatsMessage(Message message)
        {
            MsgHeader headers = new MsgHeader();// _headerBuilder?.Build(message);

            // Topic/Subject in the Msg should not include streamname, subjects are unique across streams
            Msg natsMessage = new Msg(message.Header.Topic, null, headers, message.Body.Bytes);

            return natsMessage;
        }
    }
}
