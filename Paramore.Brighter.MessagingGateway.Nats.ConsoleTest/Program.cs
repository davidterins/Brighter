using Greetings.Ports.Commands;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Nats;
using Polly;
using Polly.Registry;

namespace GreetingsSender
{
    internal static class Program
    {
        static async Task Main(string[] args)
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
                    builder.ClearProviders();
                    builder.AddConsole();
                    builder.AddDebug();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    var retryPolicy = Policy.Handle<Exception>().WaitAndRetry(new[]
                    {
                        TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100),
                        TimeSpan.FromMilliseconds(150)
                    });

                    var circuitBreakerPolicy =
                        Policy.Handle<Exception>().CircuitBreaker(1, TimeSpan.FromMilliseconds(500));

                    var retryPolicyAsync = Policy.Handle<Exception>().WaitAndRetryAsync(new[]
                    {
                        TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100),
                        TimeSpan.FromMilliseconds(150)
                    });

                    var circuitBreakerPolicyAsync = Policy.Handle<Exception>()
                        .CircuitBreakerAsync(1, TimeSpan.FromMilliseconds(500));

                    var policyRegistry = new PolicyRegistry
                    {
                        {CommandProcessor.RETRYPOLICY, retryPolicy},
                        {CommandProcessor.CIRCUITBREAKER, circuitBreakerPolicy},
                        {CommandProcessor.RETRYPOLICYASYNC, retryPolicyAsync},
                        {CommandProcessor.CIRCUITBREAKERASYNC, circuitBreakerPolicyAsync}
                    };

                    services.AddBrighter(options =>
                    {
                        options.PolicyRegistry = policyRegistry;
                    })
                        .UseInMemoryOutbox()
                        .UseExternalBus(
                            new NatsProducerRegistryFactory(
                                    new NatsMessagingGatewayConfiguration
                                    {
                                        Name = "paramore.brighter.greetingsender",
                                    },
                                    new NatsPublicationConfig[]
                                    {
                                        new NatsPublicationConfig
                                        {
                                            Topic = new RoutingKey("greeting.event"),
                                           // NumPartitions = 3,
                                           // MessageSendMaxRetries = 3,
                                           // MessageTimeoutMs = 1000,
                                           // MaxInFlightRequestsPerConnection = 1
                                        }
                                    })
                                .Create())
                        .MapperRegistryFromAssemblies(typeof(GreetingEvent).Assembly);

                    services.AddHostedService<TimedMessageGenerator>();
                })
                .UseConsoleLifetime()
                .Build();

            await host.RunAsync();
        }
    }
}

