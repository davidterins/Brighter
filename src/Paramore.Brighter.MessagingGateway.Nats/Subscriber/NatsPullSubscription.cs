using System;
namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsPullSubscription : Subscription
    {
        public NatsPullSubscription(
            Type dataType,
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
        }
    }

    public class NatsPullSubscription<T> : NatsPullSubscription where T : IRequest
    {
        public NatsPullSubscription(
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
}

