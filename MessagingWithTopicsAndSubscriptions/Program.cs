namespace MessagingWithTopicsAndSubscriptions
{
    using System;
    using System.Collections.Generic;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using System.Configuration;
    using System.Threading;

    public class program
    {
        private static string TopicName = "SampleQueue";
        private static MessagingFactory messagingFactory = MessagingFactory.Create();
        const Int16 maxTrials = 4;

        static void Main(string[] args)
        {
            // Please see http://go.microsoft.com/fwlink/?LinkID=249089 for getting Service Bus connection string and adding to app.config

            Console.WriteLine("Creating a Queue");
            CreateTopic();
            Console.WriteLine("Press anykey to start sending messages ...");
            Console.ReadKey();
            SendMessages();
            Console.WriteLine("Press anykey to start receiving messages that you just sent ...");
            Console.ReadKey();
            ReceiveMessages();
            Console.WriteLine("\nEnd of scenario, press anykey to exit.");
            Console.ReadKey();
        }

        private static void CreateTopic()
        {
            NamespaceManager namespaceManager = NamespaceManager.Create();

            Console.WriteLine("\nCreating Queue '{0}'...", TopicName);

            // Delete if exists
            if (namespaceManager.TopicExists(TopicName))
            {
                namespaceManager.DeleteQueue(TopicName);
            }

            namespaceManager.CreateQueue(TopicName);
        }

        private static void SendMessages()
        {
            var queueClient = messagingFactory.CreateQueueClient(TopicName);

            List<BrokeredMessage> messageList = new List<BrokeredMessage>();

            messageList.Add(CreateSampleMessage("1.1.1", "First message information"));
            messageList.Add(CreateSampleMessage("2.2.2", "Second message information"));
            messageList.Add(CreateSampleMessage("3.3.3", "Third message information"));

            Console.WriteLine("\nSending messages to Queue...");

            foreach (BrokeredMessage message in messageList)
            {
                while (true)
                {
                    try
                    {
                        queueClient.Send(message);
                    }
                    catch (MessagingException e)
                    {
                        if (!e.IsTransient)
                        {
                            Console.WriteLine(e.Message);
                            throw;
                        }
                        else
                        {
                            HandleTransientErrors(e);
                        }
                    }
                    Console.WriteLine(string.Format("Message sent: Id = {0}, Body = {1}", message.MessageId, message.GetBody<Version>()));
                    break;
                }
            }

            queueClient.Close();
        }

        private static void ReceiveMessages()
        {
            var queueClient = messagingFactory.CreateQueueClient(TopicName);

            Console.WriteLine("\nReceiving message from Queue...");
            BrokeredMessage message = null;
            while (true)
            {
                try
                {
                    //receive messages from Queue
                    message = queueClient.Receive(TimeSpan.FromSeconds(5));
                    if (message != null)
                    {
                        Console.WriteLine(string.Format("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<Version>()));
                        // Further custom message processing could go here...
                        message.Complete();
                    }
                    else
                    {
                        //no more messages in the queue
                        break;
                    }
                }
                catch (MessagingException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                    else
                    {
                        HandleTransientErrors(e);
                    }
                }
            }
            queueClient.Close();
        }

        private static BrokeredMessage CreateSampleMessage(string messageId, string messageBody)
        {
            BrokeredMessage message = new BrokeredMessage(new Version(messageId));
            message.MessageId = messageId;
            return message;
        }

        private static void HandleTransientErrors(MessagingException e)
        {
            //If transient error/exception, let's back-off for 2 seconds and retry
            Console.WriteLine(e.Message);
            Console.WriteLine("Will retry sending the message in 2 seconds");
            Thread.Sleep(2000);
        }
    }

}
