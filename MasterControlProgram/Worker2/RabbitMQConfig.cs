using System;
using System.Configuration;

namespace Worker2
{
    internal static class RabbitMQConfig
    {
        //internal const string RABBITMQ_QUEUENAMEHERE_NAME = "ProductId.QueueName"
        //internal const string RABBITMQ_QUEUENAME_ROUTING_KEY = "";

        internal static string HOSTNAME = ConfigurationManager.AppSettings["RabbitMQ.HostName"];
        internal static string VIRTUAL_HOST = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
        internal static string USERNAME = ConfigurationManager.AppSettings["RabbitMQ.Username"];
        internal static string PASSWORD = ConfigurationManager.AppSettings["RabbitMQ.Password"];
        internal static string EXCHANGE = ConfigurationManager.AppSettings["RabbiqMQ.Exchange"];
        internal static string QUEUE_TIMEOUT = ConfigurationManager.AppSettings["RabbitMQ.QueueTimeout"];

        internal static string WORKER2_REQUEST_QUEUE_NAME = ConfigurationManager.AppSettings["RabbitMQ.Worker2RequestQueueName"];
        internal static string WORKER2_RESPONSE_QUEUE_NAME = ConfigurationManager.AppSettings["RabbitMQ.Worker2ResponseQueueName"];
    }
}
