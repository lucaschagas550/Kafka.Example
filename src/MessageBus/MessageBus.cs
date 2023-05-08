using Confluent.Kafka;
using MessageBus.Extensions;
using MessageBus.Messages.Base;
using System.Diagnostics;
using System.Text.Json;

namespace MessageBus
{
    public class MessageBus : IMessageBus
    {
        private readonly string _bootstrapserver;

        public MessageBus(string bootstrapserver)
        {
            _bootstrapserver = bootstrapserver;
        }

        public async Task ProducerAsync<T>(string topic, T message) where T : IntegrationEvent
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapserver,
                //SecurityProtocol = SecurityProtocol.SaslSsl,
                //SaslMechanism = SaslMechanism.Plain,
                //SaslUsername="lucas",
                //SaslPassword="teste",
                //SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None, //anula o certificado se o servidor estiver em outra maquina
                Acks = Acks.All, // Eh a opcao mais consistente porque aguarda salvar mensagem no broker, realizar a copia em outro, so depois de sincronizado retorna o sucesso
            };

            #region produto simples utilizando o tipo Json
            //var payload = System.Text.Json.JsonSerializer.Serialize(message);

            //var producer = new ProducerBuilder<string, string>(config).Build();

            //var result = await producer.ProduceAsync(topic, new Message<string, string>
            //{
            //    Key = Guid.NewGuid().ToString(),
            //    Value = payload,
            //});

            //await Task.CompletedTask;
            #endregion

            var headers = new Dictionary<string, string>();
            headers["transactionId"] = Guid.NewGuid().ToString();
            //var activity = NetDevPackExtensions.StartProducer(headers, $"Producer {topic}");

            var producer = new ProducerBuilder<string, T>(config)
            .SetValueSerializer(new Serializer<T>())
            .Build();

            var result = await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                Value = message,
                Headers = headers.DictionaryToHeader()
            });

            await Task.CompletedTask;
        }


        //Consumir um topico especifico por exemplo, PersonIntegration
        public async Task ConsumerAsync<T>(string topic, Func<T, Task> onMessage, CancellationToken cancellation) where T : IntegrationEvent
        {
            _ = Task.Factory.StartNew(async () => //Start de um Thread para background
            {
                try
                {
                    var config = new ConsumerConfig
                    {
                        GroupId = "grupo-curso",
                        BootstrapServers = _bootstrapserver,
                        //SecurityProtocol = SecurityProtocol.SaslSsl,
                        //SaslMechanism = SaslMechanism.Plain,
                        //SaslUsername="lucas",
                        //SaslPassword="teste",
                        //SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.None, //anula o certificado se o servidor estiver em outra maquina
                        EnableAutoCommit = false,
                        EnablePartitionEof = true,
                    };

                    //Json
                    //using var consumer = new ConsumerBuilder<string, string>(config).Build();

                    using var consumer = new ConsumerBuilder<string, T>(config)
                        .SetValueDeserializer(new Deserializer<T>())
                        .Build();

                    consumer.Subscribe(topic);

                    //Permanece em loop enquanto não for requisitado para o token ser cancelado
                    while (!cancellation.IsCancellationRequested)
                    {
                        var result = consumer.Consume();

                        if (result.IsPartitionEOF)
                        {
                            continue;
                        }

                        var headers = result.Message.Headers.HeaderToDictionary();
                        //NetDevPackExtensions.StartConsumer(headers, $"Consumidor: {topic}");
                        //var message = JsonSerializer.Deserialize<T>(result.Message.Value);

                        await onMessage(result.Message.Value);

                        consumer.Commit();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"erro => {ex}");
                    Debug.WriteLine($"erro => {ex}");
                }
            }, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.CompletedTask;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
