using FluentAssertions;
using Messaging.Consumers;
using Messaging.Contracts;
using Messaging.Contracts.Appointments;
using Messaging.Kafka;
using Messaging.Outbox;
using Messaging.Telemetry;
using Messaging.Tests.Helpers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace Messaging.Tests;

public class MessagingLoggingTests
{
    private readonly Mock<IEventHandler<SampleEvent>> _handlerMock = new();
    private readonly Mock<IMessageProducer> _producerMock = new();
    private readonly EventHandlerRegistry _registry = new();

    [Fact]
    public async Task Dispatch_QuandoEventoForProcessado_DeveLogarRecebidoEProcessadoComCorrelationId()
    {
        _registry.Register<SampleEvent>();
        var logger = new CapturingLogger<EventDispatcher>();
        var dispatcher = CreateDispatcher(logger);
        var json = EventsFactory.EnvelopeJson(new SampleEvent(Guid.NewGuid(), "x"), "corr-42");

        await dispatcher.DispatchAsync(EventsFactory.Message(json));

        logger.Logs.Select(l => l.EventName).Should().Equal("consumer.received", "consumer.handled");
        logger.Logs.Should().OnlyContain(l => (string?)l.Properties["CorrelationId"] == "corr-42");
        logger.Logs[0].Properties["KafkaTopic"].Should().Be(Topics.Payments);
        logger.Logs[0].Properties["KafkaOffset"].Should().Be(7L);
    }

    [Fact]
    public async Task Dispatch_QuandoNaoHaHandler_DeveLogarSkipped()
    {
        var logger = new CapturingLogger<EventDispatcher>();
        var dispatcher = CreateDispatcher(logger);

        await dispatcher.DispatchAsync(EventsFactory.Message(EventsFactory.EnvelopeJson(new SampleEvent(Guid.NewGuid(), "x"))));

        logger.Logs.Select(l => l.EventName).Should().Equal("consumer.received", "consumer.skipped");
    }

    [Fact]
    public async Task Dispatch_QuandoHandlerFalhaSempre_DeveLogarRetryFailedEDeadLettered()
    {
        _registry.Register<SampleEvent>();
        _handlerMock
            .Setup(h => h.HandleAsync(It.IsAny<SampleEvent>(), It.IsAny<EventMetadata>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("falhou"));
        var logger = new CapturingLogger<EventDispatcher>();
        var dispatcher = CreateDispatcher(logger);

        await dispatcher.DispatchAsync(EventsFactory.Message(EventsFactory.EnvelopeJson(new SampleEvent(Guid.NewGuid(), "x"))));

        logger.Logs.Select(l => l.EventName).Should().Equal(
            "consumer.received", "consumer.retry", "consumer.retry", "consumer.failed", "consumer.dead_lettered");
        logger.Logs.Single(l => l.EventName == "consumer.dead_lettered").Level.Should().Be(LogLevel.Error);
        logger.Logs.Single(l => l.EventName == "consumer.failed").Exception.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public async Task Dispatch_QuandoPayloadForInvalido_DeveLogarDeadLetteredSemCorrelationId()
    {
        var logger = new CapturingLogger<EventDispatcher>();
        var dispatcher = CreateDispatcher(logger);

        await dispatcher.DispatchAsync(EventsFactory.Message("não é json"));

        var log = logger.Logs.Should().ContainSingle().Subject;
        log.EventName.Should().Be("consumer.dead_lettered");
        log.Properties.Should().NotContainKey("CorrelationId");
        log.Properties["KafkaTopic"].Should().Be(Topics.Payments);
    }

    [Fact]
    public async Task Publish_QuandoPublicaComSucesso_DeveLogarPublishedComCorrelationIdDoEvento()
    {
        var databaseName = Guid.NewGuid().ToString();
        var payload = EventsFactory.EnvelopeJson(new AppointmentRequested(EventsFactory.Snapshot()), "corr-77");
        await SeedAsync(databaseName, new OutboxMessage(Guid.CreateVersion7(), Topics.Appointments, "k1", "AppointmentRequested", payload, DateTime.UtcNow));
        var logger = new CapturingLogger<OutboxPublisherService<TestDbContext>>();
        SetupProducerSuccess();

        await CreatePublisher(databaseName, logger).PublishPendingAsync();

        var log = logger.Logs.Should().ContainSingle().Subject;
        log.EventName.Should().Be("outbox.published");
        log.Properties["CorrelationId"].Should().Be("corr-77");
        log.Properties["MessageKey"].Should().Be("k1");
        ((long)log.Properties["LagMs"]!).Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public async Task Publish_QuandoProducerFalha_DeveLogarPublishFailedComTentativas()
    {
        var databaseName = Guid.NewGuid().ToString();
        await SeedAsync(databaseName, new OutboxMessage(Guid.CreateVersion7(), Topics.Appointments, "k1", "AppointmentRequested", "{}", DateTime.UtcNow));
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<IReadOnlyDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("broker fora"));
        var logger = new CapturingLogger<OutboxPublisherService<TestDbContext>>();

        await CreatePublisher(databaseName, logger).PublishPendingAsync();

        var log = logger.Logs.Should().ContainSingle().Subject;
        log.EventName.Should().Be("outbox.publish_failed");
        log.Level.Should().Be(LogLevel.Warning);
        log.Properties["Attempts"].Should().Be(1);
        log.Exception.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public void Enqueue_DeveLogarEnqueuedComCorrelationId()
    {
        using var context = TestDbContext.Create(Guid.NewGuid().ToString());
        var logger = new CapturingLogger<EfOutbox<TestDbContext>>();
        var outbox = new EfOutbox<TestDbContext>(
            context, new Microsoft.AspNetCore.Http.HttpContextAccessor(),
            Options.Create(new KafkaOptions { ClientId = "dentist-service" }), logger);

        outbox.Enqueue(new AppointmentRequested(EventsFactory.Snapshot()), "corr-5");

        var log = logger.Logs.Should().ContainSingle().Subject;
        log.EventName.Should().Be("outbox.enqueued");
        log.Properties["CorrelationId"].Should().Be("corr-5");
        log.Properties["Topic"].Should().Be(Topics.Appointments);
        log.Properties["MessageKey"].Should().Be(EventsFactory.DefaultAppointmentId.ToString());
    }

    private EventDispatcher CreateDispatcher(ILogger<EventDispatcher> logger)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => _handlerMock.Object);
        var options = new KafkaOptions
        {
            Consumer = new KafkaConsumerOptions { GroupId = "dentist-service", MaxRetries = 2, RetryDelayMs = 0 }
        };

        return new EventDispatcher(
            _registry,
            services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _producerMock.Object,
            Options.Create(options),
            new MessagingMetrics(),
            logger);
    }

    private OutboxPublisherService<TestDbContext> CreatePublisher(string databaseName, ILogger<OutboxPublisherService<TestDbContext>> logger)
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options => options.UseInMemoryDatabase(databaseName, TestDbContext.Root));

        return new OutboxPublisherService<TestDbContext>(
            services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _producerMock.Object,
            Options.Create(new KafkaOptions { Outbox = new KafkaOutboxOptions { BatchSize = 10 } }),
            new MessagingMetrics(),
            logger);
    }

    private void SetupProducerSuccess() =>
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<IReadOnlyDictionary<string, string>?>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

    private static async Task SeedAsync(string databaseName, OutboxMessage message)
    {
        await using var context = TestDbContext.Create(databaseName);
        context.OutboxMessages.Add(message);
        await context.SaveChangesAsync();
    }
}
