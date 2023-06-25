using System.IO;
using System.Threading.Tasks;
using Greetings.Ports.CommandHandlers;
using Greetings.Ports.Commands;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.Nats;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;
using Paramore.Brighter.ServiceActivator.Extensions.Hosting;

namespace GreetingsReceiverConsole
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureHostConfiguration(configurationBuilder =>
                {
                    configurationBuilder.SetBasePath(Directory.GetCurrentDirectory());
                    configurationBuilder.AddJsonFile("appsettings.json", optional: true);
                    configurationBuilder.AddCommandLine(args);
                })
                .ConfigureLogging((context, builder) =>
                {
                    builder.AddConsole();
                    builder.AddDebug();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    var subscriptions = new NatsPullSubscription[]
                    {
                            new NatsPullSubscription<GreetingEvent>(
                                new SubscriptionName("greeting.event"),
                                routingKey: new RoutingKey("greeting.event"),
                                makeChannels: OnMissingChannel.Assume,
                                timeoutInMilliseconds: 10000),
                    };

                    //Create the gateway
                    var consumerFactory = new NatsMessageConsumerFactory(
                        new NatsMessagingGatewayConfiguration
                        {
                            Name = "paramore.brighter.greetingsReciever"
                        }
                    );

                    services.AddServiceActivator(options =>
                    {
                        options.Subscriptions = subscriptions;
                        options.ChannelFactory = new ChannelFactory(consumerFactory);
                    }).AutoFromAssemblies(typeof(GreetingEventHandler).Assembly);


                    services.AddHostedService<ServiceActivatorHostedService>();
                })
                .UseConsoleLifetime()
                .Build();

            await host.RunAsync();
        }
    }
}
