using MessageBus.Messages.Base;

namespace MessageBus.Messages.Integration
{
    public class CarIntegration : IntegrationEvent
    {
        public string Name { get; set; }
        public int Age { get; set; }

        public CarIntegration() { }

        public CarIntegration(string name, int age)
        {
            Name = name;
            Age = age;
        }
    }
}
