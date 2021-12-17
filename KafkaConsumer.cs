using System;
using System.Threading.Tasks;
using Confluent.Kafka;
namespace KafkaConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //Configuration for the Consumer
            var config = new ConsumerConfig
            {
                GroupId = "sauravtest-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            //build the Consumer for consuming the messages
            using(var c = new ConsumerBuilder<Null, string>(config).Build())
            {
                //subscribe to the topic 
                c.Subscribe("SauravTest");

                //As we know that the consumer is the blocking call so we want to capture the CTRL + C 
                //So that we can stop the consumer from consuming the further call
                var cts = new System.Threading.CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                //now try to read the messages from the Apache Kafka Broker
                try
                {
                    while (true)
                    {

                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' from topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}");
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine("Error Occured..!!");
                    Console.WriteLine("Error Message:", e.Message);
                }

            }
        }
    }
}
