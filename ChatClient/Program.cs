using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace ChatClient
{
    class Program
    {
        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.SetPrincipalPolicy(PrincipalPolicy.WindowsPrincipal);

            Console.WriteLine("What is your name?");
            var myQueue = Console.ReadLine();

            Console.WriteLine("Who do you want to talk to?");
            var hisQueue = Console.ReadLine();

            var namespaceManager = NamespaceManager.Create();
            var messagingFactory = MessagingFactory.Create();

            if (!namespaceManager.QueueExists(hisQueue))
            {
                namespaceManager.CreateQueue(hisQueue);
            }

            if (!namespaceManager.QueueExists(myQueue))
            {
                namespaceManager.CreateQueue(myQueue);
            }

            Task.Run(() =>
                {
                    var queueReceiveClient = messagingFactory.CreateQueueClient(myQueue);
                    BrokeredMessage message = null;
                    while (true)
                    {
                        //receive messages from Queue
                        message = queueReceiveClient.Receive(TimeSpan.FromHours(5));
                        if (message != null)
                        {
                            Console.WriteLine(string.Format("{0} says: {1}", hisQueue, message.GetBody<string>()));
                            // Further custom message processing could go here...
                            message.Complete();
                        }
                        else
                        {
                            //no more messages in the queue
                            break;
                        }
                    }
                    queueReceiveClient.Close();
                });

            string chatMessage = null;

            var queueSendClient = messagingFactory.CreateQueueClient(hisQueue);

            do
            {
                chatMessage = Console.ReadLine();
                if (!string.IsNullOrWhiteSpace(chatMessage))
                {
                    queueSendClient.Send(new BrokeredMessage(chatMessage));
                }
            } while (chatMessage != string.Empty);
        }
    }
}
