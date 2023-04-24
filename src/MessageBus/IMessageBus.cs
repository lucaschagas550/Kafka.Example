using MessageBus.Messages.Base;

namespace MessageBus
{
    public interface IMessageBus : IDisposable
    {
        //Kafka produtor
        Task ProducerAsync<T>(string topic, T message) where T : IntegrationEvent;

        //Kafka Consumidor
        Task ConsumerAsync<T>(string topic, Func<T, Task> onMessage, CancellationToken cancellation) where T : IntegrationEvent;
    }
}
