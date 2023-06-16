using System;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;
using NATS.Client.JetStream;
using NATS.Client;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsMessagingGateway
    {
        protected static readonly ILogger s_logger = ApplicationLogging.CreateLogger<NatsMessageProducer>();
        protected IConnection _natsServerConnection;
        protected string _streamName;
        //protected JetStreamOptions _jetStreamOptions;
        //protected StreamConfiguration _streamConfiguration;
        protected OnMissingChannel MakeChannels;
        protected RoutingKey Topic;
        protected int NumPartitions;
        protected short ReplicationFactor;
        protected int TopicFindTimeoutMs;
        protected string StreamName;
        protected string TopicName;

        protected void EnsureTopic()
        {

            switch (MakeChannels)
            {
                case OnMissingChannel.Assume:
                    return;
                case OnMissingChannel.Create:
                    // Not supported as of now...
                    throw new NotSupportedException("Creating Nats subjects, streams or both is not supported.");
                case OnMissingChannel.Validate:
                    var subjectExists = FindSubject();

                    if (subjectExists)
                        return;

                    throw new ChannelFailureException($"Subject: {Topic.Value} does not exist");
            }
        }

        private bool FindSubject()
        {
            IJetStreamManagement jetStreamManagement = _natsServerConnection.CreateJetStreamManagementContext();

            try
            {
                StreamConfiguration streamConfig = jetStreamManagement.GetStreamInfo(StreamName).Config;

                if (streamConfig.Subjects.Contains(TopicName))
                {
                    return true;
                }

                return false;
            }
            catch (Exception e)
            {
                throw new ChannelFailureException(e.Message);
            }
        }

        //private void CreateJetsStreamSubject()
        //{
        //    string natsUrl = "nats://localhost:4222"; // Replace with your NATS server URL

        //    // Create a new NATS connection
        //    using (IConnection connection = new ConnectionFactory().CreateConnection(natsUrl))
        //    {
        //        // Create the JetStream context
        //        IJetStreamManagement JetStreamManagement = connection.CreateJetStreamManagementContext(JetStreamOptions.DefaultJsOptions);


        //        // Create a new stream
        //        string streamName = "my_stream";
        //        string subject = "my_subject";

        //        StreamConfiguration streamConfig = StreamConfiguration.Builder()
        //            .WithName(streamName)
        //            .WithSubjects(subject)
        //            .WithMaxMessages(1000)
        //            .Build();

        //        try
        //        {
        //            // Create the stream
        //            JetStreamManagement.AddStream(streamConfig);

        //            Console.WriteLine("Stream created successfully!");
        //        }
        //        catch (NATSBadSubscriptionException ex)
        //        {
        //            Console.WriteLine("Error creating stream: " + ex.Message);
        //        }
        //    }
        //}

        //private void AddJetsStreamSubject()
        //{
        //    string natsUrl = "nats://localhost:4222"; // Replace with your NATS server URL

        //    // Create a new NATS connection
        //    using (IConnection connection = new ConnectionFactory().CreateConnection(natsUrl))
        //    {
        //        // Create the JetStream context
        //        IJetStreamManagement jetStreamManagement = connection.CreateJetStreamManagementContext();

        //        // Update an existing stream
        //        string streamName = "my_stream";
        //        string subjectToAdd = "additional_subject";

        //        try
        //        {
        //            // Retrieve the current stream configuration
        //            StreamConfiguration streamConfig = jetStreamManagement.GetStreamInfo(streamName).Config;

        //            streamConfig.Subjects.Add(subjectToAdd);

        //            // Update the stream with the new configuration
        //            jetStreamManagement.UpdateStream(streamConfig);

        //            Console.WriteLine("Subject added to the stream successfully!");
        //        }
        //        catch (NATSBadSubscriptionException ex)
        //        {
        //            Console.WriteLine("Error updating stream: " + ex.Message);
        //        }
        //    }
        //}

        //private async Task MakeSubject()
        //{
        //    using (var adminClient = new AdminClientBuilder(_clientConfig).Build())
        //    {
        //        try
        //        {
        //            await adminClient.CreateTopicsAsync(new List<TopicSpecification>
        //            {
        //                new TopicSpecification
        //                {
        //                    Name = Topic.Value,
        //                    NumPartitions = NumPartitions,
        //                    ReplicationFactor = ReplicationFactor
        //                }
        //            });
        //        }
        //        catch (CreateTopicsException e)
        //        {
        //            if (e.Results[0].Error.Code != ErrorCode.TopicAlreadyExists)
        //            {
        //                throw new ChannelFailureException(
        //                    $"An error occured creating topic {Topic.Value}: {e.Results[0].Error.Reason}");
        //            }

        //            s_logger.LogDebug("Topic {Topic} already exists", Topic.Value);
        //        }
        //    }
        //}
    }
}

