using Confluent.Kafka;
using System;
using System.Threading;

namespace Kafka.Consumer
{
    class Program
    {
        static void Main()
        {
            StartAsync();

            Console.ReadKey();
        }

        static void StartAsync()
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("fila_pedido");

                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        var message = consumer.Consume(cts.Token);
                        Console.WriteLine($"MSG_RECEBIDA: { message.Value }");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }

    }
}
