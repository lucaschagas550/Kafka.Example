using MessageBus.Messages.Base;

namespace MessageBus.Messages.Integration
{
    public class PersonIntegration : IntegrationEvent
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTime Date { get; set; }

        public PersonIntegration() { }

        public PersonIntegration(string name, int age, DateTime date)
        {
            Name = name;
            Age = age;
            Date = date;
        }
    }
}
