using System;
using Microsoft.Extensions.Logging;
using Paramore.Brighter.Logging;
using NATS.Client.JetStream;
using NATS.Client;
using System.Linq;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsMessagingGateway
    {
        protected static readonly ILogger s_logger = ApplicationLogging.CreateLogger<NatsMessagingGateway>();
        protected IConnection _natsServerConnection;
        protected OnMissingChannel MakeChannels;
        protected RoutingKey Subject;
        protected string StreamName;

        protected void EnsureSubject()
        {
            switch (MakeChannels)
            {
                case OnMissingChannel.Assume:
                    return;
                case OnMissingChannel.Create:
                    CreateSubjectOrThrowException();
                    return;
                case OnMissingChannel.Validate:
                    ValidateExistSubjectOrThrowException();
                    return;
            }
        }

        private void ValidateExistSubjectOrThrowException()
        {
            try
            {
                IJetStreamManagement jetStreamManagement = _natsServerConnection.CreateJetStreamManagementContext();

                StreamInfoOptions streamInfoBuilder = StreamInfoOptions.Builder()
                    .WithFilterSubjects(Subject.Value)
                    .Build();

                StreamConfiguration streamInfo = jetStreamManagement.GetStreamInfo(StreamName, streamInfoBuilder).Config;

                if (!streamInfo.Subjects.Any())
                {
                    throw new Exception($"Subject: {Subject.Value} does not exist on stream: ${StreamName}");
                }
            }
            catch (Exception e)
            {
                throw new ChannelFailureException($"Failed to validate subject, {e.Message}");
            }
        }

        private void CreateSubjectOrThrowException()
        {
            try
            {
                IJetStreamManagement jetStreamManagement = _natsServerConnection.CreateJetStreamManagementContext();

                StreamInfoOptions streamInfoBuilder = StreamInfoOptions.Builder()
                    .WithFilterSubjects(Subject.Value)
                    .Build();

                StreamConfiguration streamInfo = jetStreamManagement.GetStreamInfo(StreamName, streamInfoBuilder).Config;

                streamInfo.Subjects.Add(Subject.Value);

                jetStreamManagement.UpdateStream(streamInfo);
            }
            catch (Exception e)
            {
                throw new ChannelFailureException($"Failed to create subject: {Subject.Value} on stream: {StreamName}, {e.Message}");
            }
        }
    }
}

