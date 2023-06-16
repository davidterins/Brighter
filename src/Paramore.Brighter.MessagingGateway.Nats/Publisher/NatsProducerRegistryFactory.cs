using System;
using System.Collections.Generic;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    public class NatsProducerRegistryFactory : IAmAProducerRegistryFactory
    {
        private readonly NatsMessagingGatewayConfiguration _globalConfiguration;
        private readonly IEnumerable<NatsPublicationConfig> _publicationConfigs;

        public NatsProducerRegistryFactory(
            NatsMessagingGatewayConfiguration globalConfiguration,
            IEnumerable<NatsPublicationConfig> publicationConfigs)
        {
            _globalConfiguration = globalConfiguration;
            _publicationConfigs = publicationConfigs;
        }

        public IAmAProducerRegistry Create()
        {
            var publicationConfigsByTopic = new Dictionary<string, IAmAMessageProducer>();

            foreach (NatsPublicationConfig publicationConfig in _publicationConfigs)
            {
                var producer = new NatsMessageProducer(_globalConfiguration, publicationConfig);

                producer.Init();

                publicationConfigsByTopic[publicationConfig.Topic] = producer;
            }

            return new ProducerRegistry(publicationConfigsByTopic);
        }
    }
}
