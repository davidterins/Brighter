using System;
using System.Collections.Generic;
using System.Text;
using NATS.Client;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    internal class NatsDefaultMessageHeaderBuilder : INatsMessageHeaderBuilder
    {
        MsgHeader INatsMessageHeaderBuilder.Build(Message message)
        {
            MessageHeader brighterMessageHeader = message.Header;

            var natsMessageHeader = new MsgHeader();

            // TODO: Checkout kafka implementation if needed, headers is not a vital part of NATS messages.

            return natsMessageHeader;
        }
    }
}
