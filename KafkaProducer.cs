using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {

            //Configuration for the Producer
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            //Creating the producer that can send the message 
            var p = new ProducerBuilder<Null, string>(config).Build();

            var i = 0;
            while (true)
            {

                //construct the message to send through the producer
                var message = new Message<Null, string>
                {
                    Value = "Message Created with ID :" + i
                };
                i++;
                try
                {
                    //After the creating the message it is the time to send the message to respective topic "SauravTest"
                    var dr = await p.ProduceAsync("SauravTest", message);
                    Console.WriteLine($"Produced message '{dr.Value}' to topic {dr.Topic}, partition {dr.Partition}, offset {dr.Offset}");
                    //Make this thread sleep for 5000 milli seconds
                    Thread.Sleep(5000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Erroe Occurred..!!");
                    Console.WriteLine("The erros created is:", ex.Message);
                }

            }
        }
    }
}
