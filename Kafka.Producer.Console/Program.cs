using Confluent.Kafka;
using System;

namespace Kafka.Producer
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Digite sua mensagem para fila: ");
            string message = Console.ReadLine();

            SendMessageByKafka(message);

            Console.ReadKey();
        }

        private static string SendMessageByKafka(string message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer
                                        .ProduceAsync("fila_pedido", new Message<Null, string> { Value = message })
                                        .GetAwaiter()
                                        .GetResult();

                    return $"Mensagem { sendResult.Value } de { sendResult.TopicPartitionOffset }";
                }
                catch (ProduceException<Null, string> e)
                {
                    throw new Exception($"Delivery failed: { e.Error.Reason } "); ;
                }
            }
        }
    }
}
