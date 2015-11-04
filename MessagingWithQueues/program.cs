//---------------------------------------------------------------------------------
// Microsoft (R)  Windows Azure SDK
// Software Development Kit
// 
// Copyright (c) Microsoft Corporation. All rights reserved.  
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------

//my changes
namespace Microsoft.Samples.MessagingWithQueues
{
    using System;
    using System.Collections.Generic;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using System.Configuration;
    using System.Threading;
    using System.Security.Principal;

    public class program
    {
        private static string queueName = "SampleQueue";
        private static MessagingFactory messagingFactory;
        private static readonly string connectiongString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
        const Int16 maxTrials = 4;

        static void Main(string[] args)
        {

            messagingFactory = MessagingFactory.CreateFromConnectionString(connectiongString);
            AppDomain.CurrentDomain.SetPrincipalPolicy(PrincipalPolicy.WindowsPrincipal);

            // Please see http://go.microsoft.com/fwlink/?LinkID=249089 for getting Service Bus connection string and adding to app.config

            //Console.WriteLine("Creating a Queue");
            NamespaceManager namespaceManager = CreateQueue();
            Console.WriteLine("Press anykey to start sending messages ...");
            Console.ReadKey();
            SendMessages();
            Console.WriteLine("Press anykey to start receiving messages that you just sent ...");
            Console.ReadKey();
            ReceiveMessages();

            //DeleteQueue(namespaceManager);
            Console.WriteLine("\nEnd of scenario, press anykey to exit.");
            Console.ReadKey();
        }

        private static NamespaceManager CreateQueue()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectiongString);

            Console.WriteLine("Namespace: {0}", namespaceManager.Address);

            Console.WriteLine("\nCreating Queue '{0}'...", queueName);

            // Delete if exists
            if (namespaceManager.QueueExists(queueName))
            {
                namespaceManager.DeleteQueue(queueName);
            }

            namespaceManager.CreateQueue(queueName);

            return namespaceManager;
        }

        private static void DeleteQueue(NamespaceManager namespaceManager)
        {
            Console.WriteLine("\nRemoving Queue '{0}'...", queueName);

            // Delete if exists
            if (namespaceManager.QueueExists(queueName))
            {
                namespaceManager.DeleteQueue(queueName);
            }
        }

        private static void SendMessages()
        {
            var queueClient = messagingFactory.CreateQueueClient(queueName);

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
            var queueClient = messagingFactory.CreateQueueClient(queueName);

            Console.WriteLine("\nReceiving message from Queue...");
            BrokeredMessage message = null;
            while (true)
            {
                try
                {
                    //receive messages from Queue
                    message = queueClient.Receive(TimeSpan.FromSeconds(1));
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
