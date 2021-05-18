﻿using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;

namespace Kafka.Producer.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        [HttpPost]
        [ProducesResponseType(typeof(string), 201)]
        [ProducesResponseType(400)]
        [ProducesResponseType(500)]
        public IActionResult Post([FromBody] string msg)
        {
            return Created("", SendMessageByKafka(msg));

        }

        private string SendMessageByKafka(string message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var sendResult = producer.ProduceAsync("fila_pedido", new Message<Null, string> { }).GetAwaiter().GetResult();

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