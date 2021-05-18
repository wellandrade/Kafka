using Confluent.Kafka;
using System;

namespace Kafka.Producer
{
    class Program
    {
        static void Main()
        {
            int count = 0;

            string msg = "";

            while (count < 200)
            {
                msg = $"{ count } ==> sendo enviada para fila as { DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss") }";

                Console.WriteLine($"MSG_ENVIADA: { msg } ");

                SendMessageByKafka(msg);
                count++;
            }

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
