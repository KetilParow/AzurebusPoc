using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
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
        private const string SasConnectionstring = "Endpoint=sb://parowsexpressbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4odBGPo2Jyvkh/18r1t4Lise0VpXIRdFbQxu+ffUCtc=";
        private const string Content = @"
When I'm old and mankey,
I'll never use a hanky.
I'll wee on plants
and soil my pants
and sometimes get quite cranky.";

        static void Main(string[] args)
        {
            var client = QueueClient.CreateFromConnectionString(SasConnectionstring, QueueName);
            BrokeredMessage message = new BrokeredMessage(Content) {ContentType = "Poem",Label = "When I'm Old",SessionId = $@"{{{Guid.NewGuid()}}}" };

            client.Send(message);
        }
        //static async Task Queue()
        //{
        //    // Create management credentials
        //    TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(sasKeyName, sasKeyValue);
        //    NamespaceManager namespaceClient = new NamespaceManager(ServiceBusEnvironment.CreateServiceUri("sb", nameSpace, string.Empty), credentials);
        //}
    }
   
}
