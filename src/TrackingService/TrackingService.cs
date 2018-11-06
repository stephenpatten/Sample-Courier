
namespace TrackingService
{
    using System;
    using System.Configuration;

    using Automatonymous;

    using MassTransit;
    using MassTransit.EntityFrameworkIntegration;
    using MassTransit.EntityFrameworkIntegration.Saga;
    using MassTransit.RabbitMqTransport;
    using MassTransit.Saga;

    using Topshelf;
    using Topshelf.Logging;

    using Tracking;

    /// <summary>
    /// The tracking service.
    /// </summary>
    internal class TrackingService : ServiceControl
    {
        private readonly LogWriter _log = HostLogger.Get<TrackingService>();
        private RoutingSlipMetrics _activityMetrics;

        private IBusControl _busControl;
        private RoutingSlipStateMachine _machine;
        private RoutingSlipMetrics _metrics;
        private Lazy<ISagaRepository<RoutingSlipState>> _repository;

        public bool Start(HostControl hostControl)
        {
            _log.Info("Creating bus...");

            _metrics = new RoutingSlipMetrics("Routing Slip");
            _activityMetrics = new RoutingSlipMetrics("Validate Activity");

            _machine = new RoutingSlipStateMachine();

            SagaDbContextFactory sagaDbContextFactory =
                () => new SagaDbContext<RoutingSlipState, RoutingSlipStateSagaMap>(SagaDbContextFactoryProvider.ConnectionString);

            _repository = new Lazy<ISagaRepository<RoutingSlipState>>(
               () => new EntityFrameworkSagaRepository<RoutingSlipState>(sagaDbContextFactory));

            _busControl = Bus.Factory.CreateUsingRabbitMq(x =>
            {
                IRabbitMqHost host = x.Host(new Uri(ConfigurationManager.AppSettings["RabbitMQHost"]), h =>
                {
                    h.Username("samplecourier");
                    h.Password("samplecourier");
                });

                x.ReceiveEndpoint(host, "routing_slip_metrics", e =>
                {
                    e.PrefetchCount = 100;
                    e.UseRetry(Retry.None);
                    e.Consumer(() => new RoutingSlipMetricsConsumer(_metrics));
                });

                x.ReceiveEndpoint(host, "routing_slip_activity_metrics", e =>
                {
                    e.PrefetchCount = 100;
                    e.UseRetry(Retry.None);
                    e.Consumer(() => new RoutingSlipActivityConsumer(_activityMetrics, "Validate"));
                });

                x.ReceiveEndpoint(host, "routing_slip_state", e =>
                {
                    e.PrefetchCount = 8;
                    e.UseConcurrencyLimit(1);
                    e.StateMachineSaga(_machine, _repository.Value);
                });
            });

            _log.Info("Starting bus...");

            _busControl.StartAsync().Wait();

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            _log.Info("Stopping bus...");

            _busControl?.Stop();

            return true;
        }
    }
}