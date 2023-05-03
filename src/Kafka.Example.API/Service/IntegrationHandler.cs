using MessageBus;
using MessageBus.Messages.Integration;
using System.Diagnostics;

namespace Kafka.Example.API.Service
{
    public class IntegrationHandler : BackgroundService
    {
        private readonly IMessageBus _bus;
        private readonly IServiceProvider _serviceProvider;
        private int TotalMessagesReceived = 0;
        private DateTime _date;

        public IntegrationHandler(IServiceProvider serviceProvider, IMessageBus bus)
        {
            _serviceProvider = serviceProvider;
            _bus = bus;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //A criacao do topico precisa ser feito manualmente ou precisa enviar uma mensagem para ser criado
            await _bus.ConsumerAsync<PersonIntegration>("Person", Person, stoppingToken);
        }

        private async Task Person(PersonIntegration content)
        {
            #region
            //using (var scope = _serviceProvider.CreateScope())
            //{
            //    var productsWithAvailableStock = new List<Product>();
            //    var productRepository = scope.ServiceProvider.GetRequiredService<IProductRepository>();

            //    var productsId = string.Join(",", message.Items.Select(c => c.Key));
            //    var products = await productRepository.GetProductsById(productsId);

            //    if (products.Count != message.Items.Count)
            //    {
            //        await CancelOrderWithoutStock(message);
            //        return;
            //    }

            //    foreach (var product in products)
            //    {
            //        var productUnits = message.Items.FirstOrDefault(p => p.Key == product.Id).Value;

            //        if (product.IsAvailable(productUnits))
            //        {
            //            product.TakeFromInventory(productUnits);
            //            productsWithAvailableStock.Add(product);
            //        }
            //    }

            //    if (productsWithAvailableStock.Count != message.Items.Count)
            //    {
            //        await CancelOrderWithoutStock(message);
            //        return;
            //    }

            //    foreach (var product in productsWithAvailableStock)
            //    {
            //        productRepository.Update(product);
            //    }

            //    if (!await productRepository.UnitOfWork.Commit())
            //    {
            //        throw new DomainException($"Problems updating stock for order {message.OrderId}");
            //    }

            //    var productTaken = new OrderLoweredStockIntegrationEvent(message.CustomerId, message.OrderId);
            #endregion
            if(TotalMessagesReceived == 0)
            {
                _date = content.Date;
            }

            TotalMessagesReceived++;
            
            Debug.WriteLine($"Person => {DateTime.Now}: {content.Name} {content.Age} {content.Date} {content.Timestamp}");
            Console.WriteLine($"Person => {DateTime.Now}: {content.Name} {content.Age} {content.Timestamp} \n");

            Console.WriteLine($"Total messages Received: {TotalMessagesReceived} \n Total time: {DateTime.Now - _date}");
            Debug.WriteLine($"Total messages Received: {TotalMessagesReceived} \n Total time: {DateTime.Now - _date}");

            var car = new CarIntegration("carro1", 22);
            await _bus.ProducerAsync("Car", car);
        }
    }

    //public async Task CancelOrderWithoutStock(OrderAuthorizedIntegrationEvent message)
    //{
    //    var orderCancelled = new OrderCanceledIntegrationEvent(message.CustomerId, message.OrderId);
    //    await _bus.ProducerAsync("OrderCanceled", orderCancelled);
    //}
}
