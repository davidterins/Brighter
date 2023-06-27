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

        //private List<TopicPartition> _partitions = new List<TopicPartition>();
        //private readonly ConcurrentBag<TopicPartitionOffset> _offsetStorage = new ConcurrentBag<TopicPartitionOffset>();
        private readonly NatsMessageCreator _creator;
        private readonly string _subscriptionName;
        private bool _isSubscribed = false;
        private readonly long _maxBatchSize;
        private readonly int _readCommittedOffsetsTimeoutMs;
        private DateTime _lastFlushAt = DateTime.UtcNow;
        private readonly TimeSpan _sweepUncommittedInterval;
        private readonly SemaphoreSlim _flushToken = new SemaphoreSlim(1, 1);
        private bool _disposedValue;
        private IJetStreamPullSubscription pullSubscription;


        /// <summary>
        /// Constructs a NatsMessageConsumer using Confluent's Consumer Builder. We set up callbacks to handle assigned, revoked or lost partitions as
        /// well as errors. We handle storing and committing offsets, using a batch strategy to commit, with a sweeper thread to prevent partially complete
        /// batches lingering beyond a timeout threshold.
        /// </summary>
        /// <param name="configuration">The configuration tells us how to connect to the Broker. Required.</param>
        /// <param name="routingKey">The routing key is a the Nats Topic to consume. Required.</param>
        /// <param name="groupId">The id of the consumer group we belong to. Required.</param>
        /// <param name="offsetDefault">When connecting to a stream, and we have not offset stored, where do we begin reading?
        /// Earliest - beginning of the stream; latest - anything after we connect. Defaults to Earliest</param>
        /// <param name="sessionTimeoutMs">If we don't send a heartbeat within this interval, the broker terminates our session. Defaults to 1000ms</param>
        /// <param name="maxPollIntervalMs">Maximum interval between consumer polls, Failing to poll at this interval marks the client as failed triggering a
        /// re-balance of the group. Defaults to 1000ms.  Note that we set the Confluent clients auto store offsets and auto commit to false and handle this
        /// within Brighter. We store an offset following an Ack, and we commit offsets at a batch or sweeper interval, whichever is first.</param>
        /// <param name="isolationLevel">Affects reading of transactionally written messages. Committed only reads those committed to all nodes,
        /// uncommitted reads messages that have been written to *some* node. Defaults to ReadCommitted</param>
        /// <param name="commitBatchSize">What size does a batch grow before we write commits that we have stored. Defaults to 10. If a consumer crashes,
        /// uncommitted offsets will not have been written and will be processed by the consumer group again. Conversely a low batch size increases writes
        /// and lowers performance.</param>
        /// <param name="sweepUncommittedOffsetsIntervalMs">The sweeper ensures that partially complete batches, particularly on low throughput queues
        /// will be written. It runs after the interval and commits anything currently in the store and not committed. Defaults to 30000</param>
        /// <param name="readCommittedOffsetsTimeoutMs">Timeout when reading the committed offsets, used when closing a consumer to log where it reached.
        /// Defaults to 5000</param>
        /// <param name="numPartitions">If we are creating missing infrastructure, How many partitions should the topic have. Defaults to 1</param>
        /// <param name="partitionAssignmentStrategy">What is the strategy for assigning partitions to consumers?</param>
        /// <param name="replicationFactor">If we are creating missing infrastructure, how many in-sync replicas do we need. Defaults to 1</param>
        /// <param name="topicFindTimeoutMs">If we are checking for the existence of the topic, what is the timeout. Defaults to 10000ms</param>
        /// <param name="makeChannels">Should we create infrastructure (topics) where it does not exist or check. Defaults to Create</param>
        /// <exception cref="ConfigurationException">Throws an exception if required parameters missing</exception>
        public NatsMessagePullConsumer(
            NatsMessagingGatewayConfiguration configuration,
            SubscriptionName subscriptionName,
            RoutingKey routingKey,
            //string groupId,
            //AutoOffsetReset offsetDefault = AutoOffsetReset.Earliest,
            int sessionTimeoutMs = 10000,
            int maxPollIntervalMs = 10000,
            //IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            long commitBatchSize = 10,
            int sweepUncommittedOffsetsIntervalMs = 30000,
            int readCommittedOffsetsTimeoutMs = 5000,
            int numPartitions = 1,
            //PartitionAssignmentStrategy partitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            short replicationFactor = 1,
            int topicFindTimeoutMs = 10000,
            OnMissingChannel makeChannels = OnMissingChannel.Create
            )
        {
            if (configuration is null)
            {
                throw new ConfigurationException("You must set a NatsMessaginGatewayConfiguration to connect to a broker");
            }

            if (routingKey is null)
            {
                throw new ConfigurationException("You must set a RoutingKey as the Topic for the consumer");
            }

            Topic = routingKey;
            _subscriptionName = subscriptionName.Value;
            _maxBatchSize = commitBatchSize;
            _sweepUncommittedInterval = TimeSpan.FromMilliseconds(sweepUncommittedOffsetsIntervalMs);
            _readCommittedOffsetsTimeoutMs = readCommittedOffsetsTimeoutMs;

            s_logger.LogInformation("Nats consumer subscribing to {Subject}", Topic);

            _creator = new NatsMessageCreator();

            MakeChannels = makeChannels;
            Topic = routingKey;
            NumPartitions = numPartitions;
            ReplicationFactor = replicationFactor;
            TopicFindTimeoutMs = topicFindTimeoutMs;


            // TODO: Find somewhere to dispose the nats server connection and also pass in through ctr...
            _natsServerConnection = new ConnectionFactory().CreateConnection();
            IJetStream js = _natsServerConnection.CreateJetStreamContext();

            PullSubscribeOptions pullsubscribeOptions = PullSubscribeOptions.Builder()
                .WithDurable(_subscriptionName)
                .Build();

            pullSubscription = js.PullSubscribe(Topic.Value, pullsubscribeOptions);

            EnsureTopic();
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
            //if (!message.Header.Bag.TryGetValue(HeaderNames.PARTITION_OFFSET, out var bagData))
            //    return;

            //try
            //{
            //    var topicPartitionOffset = bagData as TopicPartitionOffset;

            //    var offset = new TopicPartitionOffset(topicPartitionOffset.TopicPartition, new Offset(topicPartitionOffset.Offset + 1));

            //    s_logger.LogInformation("Storing offset {Offset} to topic {Topic} for partition {ChannelName}",
            //        new Offset(topicPartitionOffset.Offset + 1).Value, topicPartitionOffset.TopicPartition.Topic,
            //        topicPartitionOffset.TopicPartition.Partition.Value);
            //    _consumer.StoreOffset(offset);
            //    _offsetStorage.Add(offset);

            //    if (_offsetStorage.Count % _maxBatchSize == 0)
            //        FlushOffsets();
            //    else
            //        SweepOffsets();

            //    s_logger.LogInformation("Current Nats batch count {OffsetCount} and {MaxBatchSize}", _offsetStorage.Count.ToString(), _maxBatchSize.ToString());
            //}
            //catch (TopicPartitionException tpe)
            //{
            //    var results = tpe.Results.Select(r =>
            //        $"Error committing topic {r.Topic} for partition {r.Partition.Value.ToString()} because {r.Error.Reason}");
            //    var errorString = string.Join(Environment.NewLine, results);
            //    s_logger.LogDebug("Error committing offsets: {0} {ErrorMessage}", Environment.NewLine, errorString);
            //}
        }

        /// <summary>
        /// There is no 'queue' to purge in Nats, so we treat this as moving past to the offset to the end of any assigned partitions,
        /// thus skipping over anything that exists at that point.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="requeue">if set to <c>true</c> [requeue].</param>
        public void Purge()
        {
            //if (!_consumer.Assignment.Any())
            //    return;

            //foreach (var topicPartition in _consumer.Assignment)
            //{
            //    _consumer.Seek(new TopicPartitionOffset(topicPartition, Offset.End));
            //}
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
                    pullSubscription.Pull(1);
                    Msg consumeResult = pullSubscription.NextMessage(-1);

                    if (consumeResult == null)
                    {
                        //CheckHasPartitions();

                        s_logger.LogDebug($"No messages available from Nats stream");
                        return new Message[] { new Message() };
                    }

                    consumeResult.Ack();

                    //var consumeResult = _consumer.Consume(new TimeSpan(0, 0, 0, 0, timeoutInMilliseconds));


                    //if (consumeResult.IsPartitionEOF)
                    //{
                    //    s_logger.LogDebug("Consumer {ConsumerMemberId} has reached the end of the partition", _consumer.MemberId);
                    //    return new Message[] { new Message() };
                    //}

                    s_logger.LogDebug("Usable message retrieved from Nats stream: {Request}", Encoding.UTF8.GetString(consumeResult.Data));
                    //s_logger.LogDebug("Partition: {ChannelName} Offset: {Offset} Value: {Request}", consumeResult.Partition,
                    //    consumeResult.Offset, consumeResult.Message.Value);
                    Message message = _creator.CreateMessage(consumeResult);
                    return new[] { message };

                }
                catch (Exception ex)
                {
                    return new Message[] { new Message() };
                }
            }
            //catch (ConsumeException consumeException)
            //{
            //    s_logger.LogError(consumeException,
            //        "NatsMessageConsumer: There was an error listening to topic {Topic} with groupId {ConsumerGroupId} on bootstrap servers: {Servers})",
            //        Topic, _consumerConfig.GroupId, _consumerConfig.BootstrapServers);
            //    throw new ChannelFailureException("Error connecting to Nats, see inner exception for details", consumeException);

            //}
            //catch (NatsException NatsException)
            //{
            //    s_logger.LogError(NatsException,
            //        "NatsMessageConsumer: There was an error listening to topic {Topic} with groupId {ConsumerGroupId} on bootstrap servers: {Servers})",
            //        Topic, _consumerConfig.GroupId, _consumerConfig.BootstrapServers);
            //    if (NatsException.Error.IsFatal) //this can't be recovered and requires a new consumer
            //        throw;

            //    throw new ChannelFailureException("Error connecting to Nats, see inner exception for details", NatsException);
            //}
            catch (Exception exception)
            {
                //s_logger.LogError(exception,
                //    "NatsMessageConsumer: There was an error listening to topic {Topic} with groupId {ConsumerGroupId} on bootstrap servers: {Servers})",
                //    Topic, _consumerConfig.GroupId, _consumerConfig.BootstrapServers);
                throw;
            }
        }


        /// <summary>
        /// Rejects the specified message. This is just a commit of the offset to move past the record without processing it
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="requeue">if set to <c>true</c> [requeue].</param>
        public void Reject(Message message)
        {
            Acknowledge(message);
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
                    pullSubscription.Dispose();
                    pullSubscription = null;
                    _natsServerConnection.Drain();
                    _natsServerConnection.Close();
                    _natsServerConnection.Dispose();
                    _natsServerConnection = null;
                    //_consumer.Dispose();
                    //_consumer = null;
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
    }
}

