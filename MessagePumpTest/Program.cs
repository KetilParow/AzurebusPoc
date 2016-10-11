using System;
using System.Threading;
using ConsoleTestBase;
using Microsoft.ServiceBus.Messaging;

namespace AzurebusReceiverTest
{
    public static class TestRunSettings
    {
        public static readonly TimeSpan? CompleteMessageAfterTime = TimeSpan.FromMinutes(10); //Since message received
        public static readonly TimeSpan? AutoRenewTimeout = null;
        public const int MaxConcurrentCalls = 1;
        public static readonly Action<string> Logger = Console.WriteLine;
    }

    public class Program : ConsoleTest
    {
        static void Main(string[] args)
        {
            using (CancellationTokenSource cancelSource = new CancellationTokenSource())
            {
                using (new TestReceiver(cancelSource))
                {
                    WaitForCancellation(cancelSource);
                }
            }
            ShowEndMessage();
        }
    }
    internal class TestReceiver : BrokeredMessageBaseHandler
    {
       
        public TestReceiver(CancellationTokenSource cancelSource) :base(cancelSource,TestRunSettings.CompleteMessageAfterTime,TestRunSettings.Logger)
        {
            ReceiveMessages();
        }

        private void ReceiveMessages()
        {
            _messagingFactory =
                MessagingFactory.CreateFromConnectionString(
                    "Endpoint=sb://pip-sb-eonnor-dev.servicebus.windows.net/;SharedAccessKeyName=smgexmespull;SharedAccessKey=DzNElW9G2tH1895uRIOeccm/HRGvN2/JecJHhqlKAck=");
            _busReceiver = _messagingFactory.CreateMessageReceiver("pip-int-powel-smgexmespull-in",
                ReceiveMode.PeekLock);
            var onMessageOptions = new OnMessageOptions()
            {
                AutoComplete = false,
                MaxConcurrentCalls = TestRunSettings.MaxConcurrentCalls,
            };
            onMessageOptions.ExceptionReceived += OnWorkerException;
            if (TestRunSettings.AutoRenewTimeout.HasValue)
            {
                onMessageOptions.AutoRenewTimeout = TestRunSettings.AutoRenewTimeout.Value;
            }
            _busReceiver.OnMessage(m => { DoWithMessage(m, _cancelSource.Token); }, onMessageOptions);
        }
        
        private void OnWorkerException(object sender, ExceptionReceivedEventArgs e)
        {
            if (e.Exception is OperationCanceledException)
            {
                //WriteConsoleMessage("????????????????????????????????", "Cancelled"); /*Consider this a normal incident*/
                return;
            }
            throw new NotImplementedException("Have only operation cancelled exception handling");
        }        
    }
}

