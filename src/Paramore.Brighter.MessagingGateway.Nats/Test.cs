
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Paramore.Brighter.Extensions.DependencyInjection;
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
                        SecurtiyType = "TODO"
                    },
                    new List<NatsPublicationConfig>()
                    {
                        new NatsPublicationConfig
                        {
                            MakeChannels = OnMissingChannel.Create,
                            Topic =  new RoutingKey("TheTopic"),
                        }
                    })
                .Create());


            services.AddServiceActivator(ServiceActivatorOptions =>
            {

            }).AutoFromAssemblies();
        }
    }
}
