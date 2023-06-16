
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Nats;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;

namespace Paramore.Brighter.MessagingGateway.Nats
{
    internal class Test
    {
        public Test(IServiceCollection services)
        {
            services.AddBrighter()
            .UseExternalBus(
                new NatsProducerRegistryFactory(
                    new NatsMessagingGatewayConfiguration
                    {
                        NatsServer = "nats://localhost:4222",
                        Name = "NatsTestClientApplication",
                        SecurtiyProtocol = "TODO"
                    },
                    new List<NatsPublicationConfig>()
                    {
                        new NatsPublicationConfig
                        {
                            MakeChannels = OnMissingChannel.Validate,
                            Topic =  new RoutingKey("greeting.event"),
                        }
                    })
                .Create());


            services.AddServiceActivator(ServiceActivatorOptions =>
            {

            }).AutoFromAssemblies();
        }
    }
}
