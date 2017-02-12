using System;
using System.Configuration;

namespace MasterControlProgram
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

        internal const string MCP_REQUEST_QUEUE_NAME = "MCP_Request";
        internal const string WORKER1_REQUEST_QUEUE_NAME = "Worker1_Request";
        internal const string WORKER1_RESPONSE_QUEUE_NAME = "Worker1_Response";
        internal const string WORKER2_REQUEST_QUEUE_NAME = "Worker2_Request";
        internal const string WORKER2_RESPONSE_QUEUE_NAME = "Worker2_Response";
    }
}
