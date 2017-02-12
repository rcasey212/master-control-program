using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.XPath;

using log4net;
using RabbitMQ.Client;
using Stateless;
using System.Xml.Serialization;
using System.Xml;

namespace MasterControlProgram.Jobs.JobA
{
    internal class JobA
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

        internal static JobA FromQueueMessage(IModel channel, byte[] messageBody)
        {
            XPathNavigator nav = null;
            using (MemoryStream ms = new MemoryStream(messageBody))
            {
                nav = new XPathDocument(ms).CreateNavigator();
            }

            JobA job = new JobA();
            job.Channel = channel;
            job.State = JobAState.Begin;
            job.JobXml = nav.InnerXml;
            job.JobId = Guid.NewGuid();
            job.StartTime = DateTime.UtcNow;
            job.NumSecondsToWork = int.Parse(nav.SelectSingleNode("/job/pauseForSeconds").Value);
            job.FullPathAndFilenameToSerializedJob = Path.Combine(
                Environment.CurrentDirectory, "serialized-jobs", job.JobId.ToString() + ".xml");
            return job;
        }

        internal static JobA FromSavedState(IModel channel, string pathFilename)
        {
            JobA job = new JobA();

            string[] serializedStateText = File.ReadAllText(pathFilename).Split(new char[] { ',' });

            XPathNavigator nav = null;
            using (StringReader reader = new StringReader(serializedStateText[1]))
            {
                XPathDocument xPathDoc = new XPathDocument(reader);
                nav = xPathDoc.CreateNavigator();
            }

            job.Channel = channel;
            job.State = (JobAState)Enum.Parse(typeof(JobAState), serializedStateText[0]);
            job.JobXml = nav.InnerXml;
            job.JobId = Guid.Parse(Path.GetFileNameWithoutExtension(pathFilename));
            job.StartTime = DateTime.UtcNow;
            job.NumSecondsToWork = int.Parse(nav.SelectSingleNode("/job/pauseForSeconds").Value);
            job.FullPathAndFilenameToSerializedJob = Path.Combine(
                Environment.CurrentDirectory, "serialized-jobs", job.JobId.ToString() + ".xml");
            return job;
        }

        private ILog m_log = LogManager.GetLogger(typeof(JobA));
        private StateMachine<JobAState, JobATrigger> m_stateMachine = null;

        internal IModel Channel { get; set; }
        internal ulong MessageDeliveryTag { get; set; }
        internal Guid JobId { get; set; }
        internal string JobXml { get; set; }
        internal int NumSecondsToWork { get; set; }
        internal DateTime StartTime { get; set; }
        internal DateTime FinishTime { get; set; }
        internal JobAState State { get; set; }
        internal string FullPathAndFilenameToSerializedJob { get; set; }

        internal JobA()
        {
            m_stateMachine = new StateMachine<JobAState, JobATrigger>(
                () => State,
                newState => State = newState);
            m_stateMachine.Configure(JobAState.Begin)
                .Permit(JobATrigger.Initialized, JobAState.InProgress);
            m_stateMachine.Configure(JobAState.InProgress)
                .OnEntry(() => InitializeJob())
                .Permit(JobATrigger.Worker1Start, JobAState.Worker1_InProgress);
            m_stateMachine.Configure(JobAState.Worker1_InProgress)
                .SubstateOf(JobAState.InProgress)
                .OnEntry(() => ProcessWorker1Start())
                .Permit(JobATrigger.Worker1Complete, JobAState.Worker1_Complete)
                .Permit(JobATrigger.Worker1Error, JobAState.Worker1_Error);
            m_stateMachine.Configure(JobAState.Worker1_Error)
                .OnEntry(() => ProcessWorker1Error());
            m_stateMachine.Configure(JobAState.Worker1_Complete)
                .SubstateOf(JobAState.InProgress)
                .OnEntry(() => ProcessWorker1Complete())
                .Permit(JobATrigger.Worker2Start, JobAState.Worker2_InProgress);
            m_stateMachine.Configure(JobAState.Worker2_InProgress)
                .SubstateOf(JobAState.InProgress)
                .OnEntry(() => ProcessWorker2Start())
                .Permit(JobATrigger.Worker2Complete, JobAState.Worker2_Complete)
                .Permit(JobATrigger.Worker2Error, JobAState.Worker2_Error);
            m_stateMachine.Configure(JobAState.Worker2_Error)
                .OnEntry(() => ProcessWorker2Error());
            m_stateMachine.Configure(JobAState.Worker2_Complete)
                .SubstateOf(JobAState.InProgress)
                .OnEntry(() => ProcessWorker2Complete())
                .Permit(JobATrigger.JobFinished, JobAState.Done);
            m_stateMachine.Configure(JobAState.Done)
                .OnEntry(() => FinalizeJob());
        }

        internal bool IsInState(JobAState state)
        {
            return m_stateMachine.IsInState(state);
        }

        private void ProcessWorker2Complete()
        {
            m_log.DebugFormat("Worker2 completed successfully, job_id: {0}", this.JobId.ToString());
            SaveJob();
            Fire(JobATrigger.JobFinished);
        }

        private void ProcessWorker1Complete()
        {
            m_log.DebugFormat("Worker1 Completed successfully, job_id: {0}", this.JobId.ToString());
            SaveJob();
            Fire(JobATrigger.Worker2Start);
        }

        private void ProcessWorker2Error()
        {
            throw new NotImplementedException();
        }

        private void ProcessWorker1Error()
        {
            throw new NotImplementedException();
        }

        private void ProcessWorker2Start()
        {
            m_log.DebugFormat("Sending message to Worker2, job_id: {0}", this.JobId.ToString());

            string message = string.Format(@"<job job_id=""{0}""><pauseForSeconds>{1}</pauseForSeconds></job>",
                this.JobId,
                this.NumSecondsToWork);
            byte[] body = Encoding.UTF8.GetBytes(message);

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = RabbitMQConfig.HOSTNAME;
            factory.VirtualHost = RabbitMQConfig.VIRTUAL_HOST;
            factory.UserName = RabbitMQConfig.USERNAME;
            factory.Password = RabbitMQConfig.PASSWORD;

            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                string mcpRequestQueue = channel.QueueDeclare(
                    queue: RabbitMQConfig.WORKER2_REQUEST_QUEUE_NAME,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                channel.QueueBind(mcpRequestQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER2_REQUEST_QUEUE_NAME);

                IBasicProperties props = channel.CreateBasicProperties();
                props.CorrelationId = this.JobId.ToString();
                props.ReplyToAddress = new PublicationAddress(
                    exchangeType: ExchangeType.Direct,
                    exchangeName: RabbitMQConfig.EXCHANGE,
                    routingKey: RabbitMQConfig.WORKER2_RESPONSE_QUEUE_NAME);
                props.DeliveryMode = 2;

                channel.BasicPublish(
                    exchange: RabbitMQConfig.EXCHANGE,
                    routingKey: RabbitMQConfig.WORKER2_REQUEST_QUEUE_NAME,
                    basicProperties: props,
                    body: body);
            }

            SaveJob();
        }

        private void ProcessWorker1Start()
        {
            m_log.DebugFormat("Sending message to Worker1, job_id: {0}", this.JobId.ToString());

            string message = string.Format("<job job_id=\"{0}\"><pauseForSeconds>{1}</pauseForSeconds></job>", this.JobId, this.NumSecondsToWork);
            byte[] body = Encoding.UTF8.GetBytes(message);

            ConnectionFactory factory = new ConnectionFactory();
            factory.HostName = RabbitMQConfig.HOSTNAME;
            factory.VirtualHost = RabbitMQConfig.VIRTUAL_HOST;
            factory.UserName = RabbitMQConfig.USERNAME;
            factory.Password = RabbitMQConfig.PASSWORD;

            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                string mcpRequestQueue = channel.QueueDeclare(
                    queue: RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                channel.QueueBind(mcpRequestQueue, RabbitMQConfig.EXCHANGE, RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME);

                IBasicProperties props = channel.CreateBasicProperties();
                props.CorrelationId = this.JobId.ToString();
                props.ReplyToAddress = new PublicationAddress(
                    exchangeType: ExchangeType.Direct,
                    exchangeName: RabbitMQConfig.EXCHANGE,
                    routingKey: RabbitMQConfig.WORKER1_RESPONSE_QUEUE_NAME);
                props.DeliveryMode = 2;

                channel.BasicPublish(
                    exchange: RabbitMQConfig.EXCHANGE,
                    routingKey: RabbitMQConfig.WORKER1_REQUEST_QUEUE_NAME,
                    basicProperties: props,
                    body: body);
            }

            SaveJob();
        }

        private void InitializeJob()
        {
            m_log.DebugFormat("Initialize job, job_id: {0}", this.JobId.ToString());
            SaveJob();
            Fire(JobATrigger.Worker1Start);
        }

        private void FinalizeJob()
        {
            m_log.DebugFormat("Finalizing job, job_id: {0}", this.JobId.ToString());
            this.FinishTime = DateTime.UtcNow;
            DeleteJob();
        }

        internal void Fire(JobATrigger trigger)
        {
            m_stateMachine.Fire(trigger);
        }

        internal void SaveJob()
        {
            if (!Directory.Exists(Path.Combine(Environment.CurrentDirectory, "serialized-jobs")))
            {
                Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "serialized-jobs"));
            }

            string text = string.Concat(this.State.ToString(), ",", this.JobXml);
            File.WriteAllText(this.FullPathAndFilenameToSerializedJob, text);
        }

        internal void DeleteJob()
        {
            File.Delete(this.FullPathAndFilenameToSerializedJob);
        }
    }
}
