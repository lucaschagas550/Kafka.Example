using MessageBus;
using MessageBus.Messages.Integration;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace Kafka.Example.API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly IMessageBus _bus;
        public WeatherForecastController(ILogger<WeatherForecastController> logger, IMessageBus bus)
        {
            _logger = logger;
            _bus = bus; 
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public async Task<IEnumerable<WeatherForecast>> Get()
        {
            await _bus.ProducerAsync("Person", new PersonIntegration("Lucas",25, DateTime.Now));

            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpGet("GetTotalMessages")]
        public async Task<IEnumerable<WeatherForecast>> GetTotalMessages()
        {
            var count = 0;

            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();

            while (sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                count++;
                await _bus.ProducerAsync("Person", new PersonIntegration("Lucas", 25, DateTime.Now));
            }

            sw.Stop();

            Console.WriteLine($"\n Total messages sent: {count} \n");
            Debug.WriteLine($"\n Total messages sent: {count} \n");

            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            }).ToArray();
        }

        [HttpGet("GetTrue")]
        public async Task<IEnumerable<WeatherForecast>> GetTrue()
        {
            while (true)
            {
                await _bus.ProducerAsync("Person", new PersonIntegration("Lucas", 25, DateTime.Now));
            }

            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            }).ToArray();
        }
    }
}