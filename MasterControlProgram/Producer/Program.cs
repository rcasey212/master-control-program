using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using log4net;
using log4net.Config;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Producer
{
    class Program
    {
        private static ILog m_log = null;

        static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            m_log = LogManager.GetLogger(typeof(Program));
            Program program = new Program();

            try
            {
                int numMessagesToSend = Program.GetNumMessages(args);
                program.SendMessages(numMessagesToSend);
            }
            catch (Exception ex)
            {
                m_log.Error(ex);
            }
        }

        private static int GetNumMessages(string[] args)
        {
            return ((args.Length > 0) ? int.Parse(args[0]) : 1);
        }

        private void SendMessages(int numMessagesToSend)
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = RabbitMQConfig.HOSTNAME;
            factory.VirtualHost = RabbitMQConfig.VIRTUAL_HOST;
            factory.UserName = RabbitMQConfig.USERNAME;
            factory.Password = RabbitMQConfig.PASSWORD;

            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.QueueDeclare(
                    queue: RabbitMQConfig.MCP_REQUEST_QUEUE_NAME,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                IBasicProperties channelProps = channel.CreateBasicProperties();
                channelProps.Persistent = true;

                Random rand = new Random();

                for (int i = 0; i < numMessagesToSend; i++)
                {
                    //int workForSeconds = rand.Next() % 5;
                    int workForSeconds = 1;
                    string message = string.Format("<job><pauseForSeconds>{0}</pauseForSeconds></job>",
                        workForSeconds.ToString());
                    byte[] body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(
                        exchange: RabbitMQConfig.EXCHANGE,
                        routingKey: RabbitMQConfig.MCP_REQUEST_QUEUE_NAME,
                        basicProperties: channelProps,
                        body: body);
                }
            }

            Console.WriteLine("Sent {0} message(s).", numMessagesToSend);
        }
    }
}
