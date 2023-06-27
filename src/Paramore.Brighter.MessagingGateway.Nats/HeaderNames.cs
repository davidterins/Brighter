using System;
using System.Collections.Generic;
using System.Text;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    internal static class HeaderNames
    {
        /// <summary>
        /// What is the content type of the message§
        /// </summary>
        public static string CONTENT_TYPE { get; set; } = "ContentType";

        /// <summary>
        /// The correlation id
        /// </summary>
        public const string CORRELATION_ID = "CorrelationId";

        /// <summary>
        /// The message type
        /// </summary>
        public const string MESSAGE_TYPE = "MessageType";

        /// <summary>
        /// The message identifier
        /// </summary>
        public const string MESSAGE_ID = "MessageId";

        /// <summary>
        /// Used for a request-reply message to indicate the private channel to reply to
        /// </summary>
        public const string REPLY_TO = "ReplyTo";
        /// <summary>
        /// The key used to partition this message
        /// </summary>
        public static string PARTITIONKEY = "PartitionKey";

        /// <summary>
        /// What is the offset into the partition of the message
        /// </summary>
        public static string PARTITION_OFFSET = "TopicPartitionOffset";

        /// <summary>
        /// The time that message was sent
        /// </summary>
        public const string TIMESTAMP = "TimeStamp";

        /// <summary>
        /// The topic
        /// </summary>
        public const string TOPIC = "Topic";
    }
}
