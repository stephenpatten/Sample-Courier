
namespace ProcessingService
{
    using System;
    using System.Configuration;
    using System.Threading;

    using GreenPipes;

    using MassTransit;
    using MassTransit.Courier.Factories;
    using MassTransit.RabbitMqTransport;

    using Processing.Activities.Retrieve;
    using Processing.Activities.Validate;

    using Topshelf;
    using Topshelf.Logging;

    internal class ActivityService : ServiceControl
    {
        private readonly LogWriter _log = HostLogger.Get<ActivityService>();

        private IBusControl _busControl;

        public bool Start(HostControl hostControl)
        {
            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine("Min: {0}", workerThreads);

            ThreadPool.SetMinThreads(200, completionPortThreads);

            this._log.Info("Creating bus...");

            this._busControl = Bus.Factory.CreateUsingRabbitMq(
                x =>
                    {
                        IRabbitMqHost host = x.Host(
                            new Uri(ConfigurationManager.AppSettings["RabbitMQHost"]),
                            h =>
                                {
                                    h.Username("samplecourier");
                                    h.Password("samplecourier");
                                });

                        x.ReceiveEndpoint(
                            host,
                            ConfigurationManager.AppSettings["ValidateActivityQueue"],
                            e =>
                                {
                                    e.PrefetchCount = 100;
                                    e.ExecuteActivityHost<ValidateActivity, ValidateArguments>(
                                        DefaultConstructorExecuteActivityFactory<ValidateActivity, ValidateArguments>
                                            .ExecuteFactory);
                                });

                        string compQueue = ConfigurationManager.AppSettings["CompensateRetrieveActivityQueue"];

                        Uri compAddress = host.GetSendAddress(compQueue);

                        x.ReceiveEndpoint(
                            host,
                            ConfigurationManager.AppSettings["RetrieveActivityQueue"],
                            e =>
                                {
                                    e.PrefetchCount = 100;
                                    e.ExecuteActivityHost<RetrieveActivity, RetrieveArguments>(
                                        compAddress,
                                        h => { h.UseRetry(rcfg => { rcfg.Interval(5, TimeSpan.FromSeconds(1)); }); });
                                });

                        x.ReceiveEndpoint(
                            host,
                            ConfigurationManager.AppSettings["CompensateRetrieveActivityQueue"],
                            e => { e.CompensateActivityHost<RetrieveActivity, RetrieveLog>(); });
                    });

            this._log.Info("Starting bus...");

            this._busControl.StartAsync().Wait();

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            this._log.Info("Stopping bus...");

            this._busControl?.Stop();

            return true;
        }
    }
}