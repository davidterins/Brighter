using System;
namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsSubscriptionConfig : Subscription
    {
        public NatsSubscriptionConfig(
            Type dataType,
            string streamName = "",
            int subjectFindTimoutMs = 300,
            SubscriptionName name = null,
            ChannelName channelName = null,
            RoutingKey routingKey = null,
            int bufferSize = 1,
            int noOfPerformers = 1,
            int timeoutInMilliseconds = 300,
            int requeueCount = -1,
            int requeueDelayInMilliseconds = 0,
            int unacceptableMessageLimit = 0,
            bool runAsync = false,
            IAmAChannelFactory channelFactory = null,
            OnMissingChannel makeChannels = OnMissingChannel.Create,
            int emptyChannelDelay = 500,
            int channelFailureDelay = 1000)
            : base(dataType, name, channelName, routingKey, bufferSize, noOfPerformers, timeoutInMilliseconds, requeueCount, requeueDelayInMilliseconds, unacceptableMessageLimit, runAsync, channelFactory, makeChannels, emptyChannelDelay, channelFailureDelay)
        {
            StreamName = streamName;
            SubjectFindTimoutMs = subjectFindTimoutMs;
        }

        /// <summary>
        /// Name of the stream, this property needs to be set in case <see cref="OnMissingChannel.Create"/> is used in order to create the subject on the correct stream.
        /// </summary>
        public string StreamName { get; init; }

        public int SubjectFindTimoutMs { get; init; } = 300;
    }

    public class NatsSubscriptionConfig<T> : NatsSubscriptionConfig where T : IRequest
    {
        public NatsSubscriptionConfig(
             SubscriptionName name = null,
             ChannelName channelName = null,
             RoutingKey routingKey = null,
             int bufferSize = 1,
             int noOfPerformers = 1,
             int timeoutInMilliseconds = 300,
             int requeueCount = -1,
             int requeueDelayInMilliseconds = 0,
             int unacceptableMessageLimit = 0,
             bool runAsync = false,
             IAmAChannelFactory channelFactory = null,
             OnMissingChannel makeChannels = OnMissingChannel.Create,
             int emptyChannelDelay = 500,
             int channelFailureDelay = 1000)
             : base(typeof(T), name, channelName, routingKey, bufferSize, noOfPerformers, timeoutInMilliseconds, requeueCount, requeueDelayInMilliseconds, unacceptableMessageLimit, runAsync, channelFactory, makeChannels, emptyChannelDelay, channelFailureDelay)
        {
        }
    }

    /// <summary>
    /// Constructs a NatsMessageConsumer using Confluent's Consumer Builder. We set up callbacks to handle assigned, revoked or lost partitions as
    /// well as errors. We handle storing and committing offsets, using a batch strategy to commit, with a sweeper thread to prevent partially complete
    /// batches lingering beyond a timeout threshold.
    /// </summary>
    /// <param name="gatewayConfiguration">The configuration tells us how to connect to the Broker. Required.</param>
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
}



