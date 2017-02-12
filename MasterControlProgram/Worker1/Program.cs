using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.XPath;

using log4net;
using log4net.Config;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Worker1
{
    /*
     * Format of request queue message:
     * <job job_id="{GUID}">
     *   <pauseForSeconds>{numberOfSecondsToDoWork}</pauseForSeconds>
     * </job>
     *
     * Format of response queue message:
     * <job job_id="{GUID}">
     *   <status>{bool: 0: false, 1: true}</status>
     * </job>
     */
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
                m_log.Info("Starting...");
                program.GetMessages();
            }
            catch (Exception ex)
            {
                m_log.Error(ex);
            }
        }

        private void GetMessages()
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = RabbitMQConfig.HOSTNAME;
            factory.VirtualHost = RabbitMQConfig.VIRTUAL_HOST;
            factory.UserName = RabbitMQConfig.USERNAME;
            factory.Password = RabbitMQConfig.PASSWORD;
            // In the event of network connection failure,
            // attempt network recovery every 5 seconds
            factory.AutomaticRecoveryEnabled = false;
            //factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(5);

            while (true)
            {
                try
                {
                    using (IConnection connection = factory.CreateConnection())
                    using (IModel channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(
                            queue: RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

                        channel.BasicQos(
                            prefetchCount: 1,
                            prefetchSize: 0,
                            global: false);

                        QueueingBasicConsumer consumer = new QueueingBasicConsumer(channel);
                        channel.BasicConsume(RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME, false, consumer);

                        while (connection != null && connection.IsOpen && channel != null && channel.IsOpen)
                        {
                            m_log.Debug("Waiting for messages...");

                            BasicDeliverEventArgs ea = null;
                            if (consumer.Queue.Dequeue(int.Parse(RabbitMQConfig.QUEUE_TIMEOUT), out ea))
                            {
                                // Parse message
                                int numSecondsToWork = 0;
                                Guid jobId = Guid.Empty;

                                XPathNavigator nav = null;
                                using (MemoryStream ms = new MemoryStream(ea.Body))
                                {
                                    nav = new XPathDocument(ms).CreateNavigator();
                                    jobId = Guid.Parse(nav.SelectSingleNode("/job/@job_id").Value);
                                    numSecondsToWork = int.Parse(nav.SelectSingleNode("/job/pauseForSeconds").Value);
                                    m_log.DebugFormat("Received message, job_id: {0}, workForSeconds: {1}.",
                                        jobId.ToString(), numSecondsToWork);
                                }

                                // Do work and get success status
                                bool isWorkSuccess = DoWork(jobId, numSecondsToWork);

                                // Send status message to reply-to address with success/failure of work
                                SendResponseMessage(channel, ea.BasicProperties.ReplyToAddress, ea.BasicProperties.CorrelationId, jobId, isWorkSuccess);

                                // Acknowledge request message - all work has been successfully completed
                                channel.BasicAck(ea.DeliveryTag, false);
                                m_log.Debug("Sent request message acknowledgement.");
                            }
                        }

                        consumer = null;
                    }
                }

                catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException)
                {
                    m_log.Error("Could not connect to the message queue service.  Will attempt to connect indefinitely...");
                    Thread.Sleep(2000);
                }
                catch (Exception ex)
                {
                    if (ex is EndOfStreamException || ex is RabbitMQ.Client.Exceptions.OperationInterruptedException)
                    {
                        m_log.Error("Connection to the message queue service was lost.");
                    }

                    else
                    {
                        m_log.Error(ex);
                    }
                }
            }
        }

        private void SendResponseMessage(IModel channel, PublicationAddress replyToAddress, string correlationId, Guid jobId, bool isWorkSuccess)
        {
            string replyMessage = string.Format(@"<job job_id=""{0}""><status>{1}</status></job>",
                jobId.ToString(),
                isWorkSuccess.ToString());
            byte[] replyBody = Encoding.UTF8.GetBytes(replyMessage);

            IBasicProperties props = channel.CreateBasicProperties();
            if (null != correlationId)
            {
                props.CorrelationId = correlationId;
            }

            channel.BasicPublish(replyToAddress, props, replyBody);
            m_log.DebugFormat("Sent response message, job_id: {0}", jobId.ToString());
        }

        private bool DoWork(Guid jobId, int numSecondsToWork)
        {
            // This is a stub for doing real work.  Just pausing for the requested amount of seconds.

            try
            {
                m_log.DebugFormat("Doing work for {0} seconds, job_id: {1}", numSecondsToWork, jobId.ToString());
                Thread.Sleep(numSecondsToWork * 1000);

                return true;
            }
            catch (Exception ex)
            {
                m_log.Error(ex);
                return false;
            }
        }

        //private void Consumer_Received(object sender, BasicDeliverEventArgs ea)
        //{
        //    m_log.Debug("Received message...");

        //    Random rand = new Random();
        //    int numSecondsToDoWork = (rand.Next() % 5);

        //    byte[] body = ea.Body;
        //    string message = Encoding.UTF8.GetString(body);

        //    m_log.InfoFormat("Received message: {0}\nWill do work for {1} seconds.", message, numSecondsToDoWork);
        //    Thread.Sleep(TimeSpan.FromSeconds(numSecondsToDoWork));
        //    m_log.Info("Completed doing work.");

        //    m_log.Debug("Attempting to acknowledge message to queue...");
        //    EventingBasicConsumer consumer = (EventingBasicConsumer)sender;
        //    consumer.Model.BasicAck(
        //        deliveryTag: ea.DeliveryTag,
        //        multiple: false);
        //    m_log.Info("Acknowledged message to queue.");
        //}
    }
}
