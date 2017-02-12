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

using MasterControlProgram.Jobs.JobA;
using System.Xml.XPath;
using System.IO;
using System.Xml.Serialization;
using System.Threading;

namespace MasterControlProgram
{
    class Program
    {
        private static ILog m_log = null;

        private static IModel m_channel = null;
        private static List<JobA> m_jobs_JobA = new List<JobA>();

        static void Main(string[] args)
        {
            XmlConfigurator.Configure();
            m_log = LogManager.GetLogger(typeof(Program));
            Program program = new Program();

            try
            {
                program.RunForever(args);
            }
            catch (Exception ex)
            {
                m_log.Error(ex);
            }
        }

        private void RunForever(string[] args)
        {
            while (true)
            {
                try
                {
                    Run(args);
                }
                catch (AlreadyClosedException ex)
                {
                    m_log.WarnFormat("MasterControlProgram: The RabbitMQ connection/session was closed unexpectedly.  It will be automatically restarted.\n{0}", ex);
                }
                catch (BrokerUnreachableException ex)
                {
                    m_log.ErrorFormat("MasterControlProgram: Could not create a RabbitMQ connection/session.  Will attempt to re-connect indefinitely.  {0}", ex);
                }
            }
        }

        private void Run(string[] args)
        {
            // This application must monitor multiple queues and figure out
            // how to move the production work item forward based on which
            // queue sends a response.
            //
            // Here are the queues it needs to montior:
            // - The incoming request queue for newly submitted jobs
            // - The Worker1 response queue
            // - The Worker2 response queue

            QueueingBasicConsumer consumer = ConnectToMQ();

            LoadUncompletedJobs();

            while (true)
            {
                m_log.Info("Waiting for queue message...");

                BasicDeliverEventArgs ea;

                try
                {
                    if (consumer.Queue.Dequeue((int)TimeSpan.FromMilliseconds(500d).TotalMilliseconds, out ea))
                    {
                        // Process the message
                        //
                        // NOTE: It is the responsibility of these functions
                        // to send reply-to messages
                        switch (ea.RoutingKey)
                        {
                            case RabbitMQConfig.MCP_REQUEST_QUEUE_NAME:
                                ProcessMCPRequestQueueMessage(m_channel, ea);
                                break;
                            case RabbitMQConfig.WORKER1_RESPONSE_QUEUE_NAME:
                                ProcessWorker1ResponseQueueMessage(m_channel, ea);
                                break;
                            case RabbitMQConfig.WORKER2_RESPONSE_QUEUE_NAME:
                                ProcessWorker2ResponseQueueMessage(m_channel, ea);
                                break;
                        }
                    }

                    CleanupOldJobs();
                    LogStatus();
                }

                catch (EndOfStreamException eosEx)
                {
                    m_log.Warn("Connection to the queue was lost.");
                    consumer = ConnectToMQ();
                }
            }
        }

        private void CleanupOldJobs()
        {
            // Remove done jobs that finished more than an hour ago
            m_jobs_JobA.RemoveAll(j => j.State == JobAState.Done && j.FinishTime < DateTime.UtcNow.AddMinutes(-2));
        }

        private void LogStatus()
        {
            if (m_log.IsInfoEnabled)
            {
                int numJobs = m_jobs_JobA.Count();
                int numInProgressJobs = m_jobs_JobA.Where(j => j.IsInState(JobAState.InProgress) == true).Count();
                int numDoneJobs = m_jobs_JobA.Where(j => j.State == JobAState.Done).Count();
                m_log.InfoFormat("# jobs: {0}, # jobs in progress: {1}, # jobs done: {2}", numJobs, numInProgressJobs, numDoneJobs);
            }
        }

        private void LoadUncompletedJobs()
        {
            string directoryToSavedStates = Path.Combine(Environment.CurrentDirectory, "serialized-jobs");

            if (!Directory.Exists(directoryToSavedStates))
            {
                Directory.CreateDirectory(directoryToSavedStates);
            }

            string[] stateFilenames = Directory.GetFiles(directoryToSavedStates);
            int numStateFiles = stateFilenames.Count();

            if (numStateFiles > 0)
            {
                m_log.DebugFormat("Loading {0} uncompleted jobs from disk...", numStateFiles);

                foreach (string filename in stateFilenames)
                {
                    JobA job = JobA.FromSavedState(m_channel, filename);
                    m_jobs_JobA.Add(job);

                    m_log.DebugFormat("Loaded job from disk, job_id: {0}", job.JobId);
                }
            }
        }

        private void ProcessWorker2ResponseQueueMessage(IModel channel, BasicDeliverEventArgs ea)
        {
            XPathNavigator navResponse = null;
            using (MemoryStream ms = new MemoryStream(ea.Body))
            {
                navResponse = new XPathDocument(ms).CreateNavigator();
            }

            Guid jobId = Guid.Parse(ea.BasicProperties.CorrelationId);
            JobA job = null;

            job = m_jobs_JobA.Where(j => j.JobId == jobId).FirstOrDefault();

            if (null == job)
            {
                m_log.WarnFormat("Received a Worker2 response for an unrecognized job, job_id: {0}.  Ignoring...", jobId);
                return;
            }
            if (JobAState.Done == job.State)
            {
                m_log.WarnFormat("Received a Worker2 response for a completed job, job_id: {0}.  Ignoring...", jobId);
                return;
            }

            // Get status from Worker1 response message
            bool status = bool.Parse(navResponse.SelectSingleNode("/job/status").Value);

            switch (status)
            {
                case false:
                    job.Fire(JobATrigger.Worker2Error);
                    break;
                case true:
                    job.Fire(JobATrigger.Worker2Complete);
                    break;
            }

            // Acknowledge the message was processed
            channel.BasicAck(
                deliveryTag: ea.DeliveryTag,
                multiple: false);
        }

        private void ProcessWorker1ResponseQueueMessage(IModel channel, BasicDeliverEventArgs ea)
        {
            XPathNavigator navResponse = null;
            using (MemoryStream ms = new MemoryStream(ea.Body))
            {
                navResponse = new XPathDocument(ms).CreateNavigator();
            }

            Guid jobId = Guid.Parse(ea.BasicProperties.CorrelationId);
            JobA job = null;

            job = m_jobs_JobA.Where(j => j.JobId == jobId).FirstOrDefault();

            if (null == job)
            {
                m_log.WarnFormat("Received a Worker1 response for an unrecognized job, job_id: {0}.  Ignoring...", jobId);
                return;
            }
            if (JobAState.Done == job.State)
            {
                m_log.WarnFormat("Received a Worker1 response for a completed job, job_id: {0}.  Ignoring...", jobId);
                return;
            }

            // Get status from Worker1 response message
            bool status = bool.Parse(navResponse.SelectSingleNode("/job/status").Value);

            switch (status)
            {
                case false:
                    job.Fire(JobATrigger.Worker1Error);
                    break;
                case true:
                    job.Fire(JobATrigger.Worker1Complete);
                    break;
            }

            // Acknowledge the message was processed
            channel.BasicAck(
                deliveryTag: ea.DeliveryTag,
                multiple: false);
        }

        private void ProcessMCPRequestQueueMessage(IModel channel, BasicDeliverEventArgs ea)
        {
            JobA job = JobA.FromQueueMessage(m_channel, ea.Body);
            m_jobs_JobA.Add(job);
            m_log.DebugFormat("Created job {0}", job.JobId.ToString());
            job.Fire(JobATrigger.Initialized);

            // Acknowledge the message was processed
            m_channel.BasicAck(
                deliveryTag: ea.DeliveryTag,
                multiple: false);
        }

        private QueueingBasicConsumer ConnectToMQ()
        {
            IConnection connection = null;
            QueueingBasicConsumer consumer = null;

            while (null == m_channel || !m_channel.IsOpen)
            {
                try
                {
                    // Cleanup memory
                    if (null != m_channel)
                    {
                        m_channel.Dispose();
                        m_channel = null;
                    }
                    if (null != connection)
                    {
                        connection.Dispose();
                        connection = null;
                    }
                    if (null != consumer)
                    {
                        consumer = null;
                    }

                    ConnectionFactory factory = new ConnectionFactory();
                    factory.HostName = RabbitMQConfig.HOSTNAME;
                    factory.VirtualHost = RabbitMQConfig.VIRTUAL_HOST;
                    factory.UserName = RabbitMQConfig.USERNAME;
                    factory.Password = RabbitMQConfig.PASSWORD;

                    // In the event of network connection failure,
                    // attempt network recovery every 5 seconds
                    factory.AutomaticRecoveryEnabled = false;
                    //factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(5);

                    connection = factory.CreateConnection();
                    m_channel = connection.CreateModel();

                    string mcpRequestQueue = m_channel.QueueDeclare(
                        queue: RabbitMQConfig.MCP_REQUEST_QUEUE_NAME,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    m_channel.QueueBind(mcpRequestQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.MCP_REQUEST_QUEUE_NAME);

                    string worker1RequestQueue = m_channel.QueueDeclare(
                            queue: RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                    m_channel.QueueBind(worker1RequestQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME);

                    string worker1ResponseQueue = m_channel.QueueDeclare(
                            queue: RabbitMQConfig.WORKER1_RESPONSE_QUEUE_NAME,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                    m_channel.QueueBind(worker1ResponseQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER1_RESPONSE_QUEUE_NAME);

                    string worker2RequestQueue = m_channel.QueueDeclare(
                            queue: RabbitMQConfig.WORKER2_REQUEST_QUEUE_NAME,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                    m_channel.QueueBind(worker2RequestQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER2_REQUEST_QUEUE_NAME);

                    string worker2ResponseQueue = m_channel.QueueDeclare(
                            queue: RabbitMQConfig.WORKER2_RESPONSE_QUEUE_NAME,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                    m_channel.QueueBind(worker1ResponseQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER2_RESPONSE_QUEUE_NAME);

                    consumer = new QueueingBasicConsumer(m_channel);
                    m_channel.BasicConsume(
                        queue: RabbitMQConfig.MCP_REQUEST_QUEUE_NAME,
                        noAck: false,
                        consumer: consumer);
                    m_channel.BasicConsume(
                        queue: RabbitMQConfig.WORKER1_RESPONSE_QUEUE_NAME,
                        noAck: false,
                        consumer: consumer);
                    m_channel.BasicConsume(
                        queue: RabbitMQConfig.WORKER2_RESPONSE_QUEUE_NAME,
                        noAck: false,
                        consumer: consumer);
                }

                catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException)
                {
                    m_log.Error("Connection to the message queue service was lost.  Will attempt to reconnect indefinitely...");

                    if (null != m_channel)
                    {
                        m_channel.Dispose();
                        m_channel = null;
                    }

                    if (null != connection)
                    {
                        connection.Dispose();
                        connection = null;
                    }

                    if (null != consumer)
                    {
                        consumer = null;
                    }

                    Thread.Sleep(2000);
                }
            }

            return consumer;
        }
    }
}
