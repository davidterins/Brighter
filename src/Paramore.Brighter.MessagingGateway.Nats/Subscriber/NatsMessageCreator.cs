using System;
using System.Linq.Expressions;
using NATS.Client;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsMessageCreator
    {
        public Message CreateMessage(Msg msg)
        {
            MessageHeader messageHeaders = MapMessageHeaders(msg);
            MessageBody messageBody = MapMessageBody(msg);
            
            return new Message(messageHeaders, messageBody);
        }

        private MessageHeader MapMessageHeaders(Msg msg)
        {
            // Sync this the the nats message header builder.
            return new MessageHeader(Guid.NewGuid(), msg.Subject, MessageType.MT_EVENT);
        }

        private MessageBody MapMessageBody(Msg msg)
        {
            return new MessageBody(msg.Data);
        }
    }
}

