using System;
using Microsoft.ServiceBus.Messaging;

namespace SendStuffTest
{
    public class Program
    {
        //private static string ServiceNamespace;
        //private static string nameSpace = "ParowsExpressBus";
        private const string QueueName = "ReadMyStuff";
        //private static string sasKeyName = "RootManageSharedAccessKey";
        //private static string sasKeyValue = "4odBGPo2Jyvkh/18r1t4Lise0VpXIRdFbQxu+ffUCtc=";
        private const string SasConnectionstring =
            "Endpoint=sb://parowsexpressbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4odBGPo2Jyvkh/18r1t4Lise0VpXIRdFbQxu+ffUCtc=";

        static void Main(string[] args)
        {
            var client = QueueClient.CreateFromConnectionString(SasConnectionstring, QueueName);
            MessageSession session = null;
            do
            {
                session = client.AcceptMessageSession(TimeSpan.FromSeconds(20));
                BrokeredMessage message = null;
                do
                {
                    message = session.Receive(TimeSpan.FromSeconds(20));
                    if (message != null)
                    {
                        Console.WriteLine(
                            $@"Message# {message.MessageId} Received.
Label: {message.Label} ({message.ContentType})
Body:
-----------------------------
{message
                                .GetBody<string>()}
-----------------------------
");
                        message.Complete();
                    }

                } while (message != null);
                Console.ReadLine();
            } while (session != null);

            //static async Task Queue()
            //{
            //    // Create management credentials
            //    TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(sasKeyName, sasKeyValue);
            //    NamespaceManager namespaceClient = new NamespaceManager(ServiceBusEnvironment.CreateServiceUri("sb", nameSpace, string.Empty), credentials);
            //}
        }

    }
}
