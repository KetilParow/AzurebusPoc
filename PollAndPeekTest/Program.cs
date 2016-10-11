using System;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using ConsoleTestBase;
using Microsoft.ServiceBus.Messaging;
using Timer = System.Timers.Timer;

namespace PollAndPeekTest
{
    internal static class TestRunSettings
    {
        public static readonly TimeSpan? CompleteMessageAfterTime = TimeSpan.FromMinutes(20); //Since message received
        //public static readonly TimeSpan? AutoRenewTimeout = null;
        public static readonly Action<string> Logger = Console.WriteLine;
        public static TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);
        //public const int MaxConcurrentCalls = 3;
    }

    public class Program : ConsoleTest
    {
        static void Main(string[] args)
        {
            CancellationTokenSource cancelSource;
            using (cancelSource = new CancellationTokenSource())
            {
                Task t = Task.Run(() => new PollReceiver(cancelSource), cancelSource.Token);                
                WaitForCancellation(cancelSource);
                if (t.Status == TaskStatus.Running && cancelSource.Token.IsCancellationRequested)
                {
                    try { t.Wait(cancelSource.Token);} catch(OperationCanceledException) { };
                }
            }
            ShowEndMessage();
        }
    }

    internal class PollReceiver : BrokeredMessageBaseHandler 
    {
        public PollReceiver(CancellationTokenSource cancelSource) :base(cancelSource, TestRunSettings.CompleteMessageAfterTime, TestRunSettings.Logger)
        {
            ReceiveMessages();
        }

        private void ReceiveMessages()
        {
            _messagingFactory = MessagingFactory.CreateFromConnectionString("Endpoint=sb://pip-sb-eonnor-dev.servicebus.windows.net/;SharedAccessKeyName=smgexmespull;SharedAccessKey=DzNElW9G2tH1895uRIOeccm/HRGvN2/JecJHhqlKAck=");
            _busReceiver = _messagingFactory.CreateMessageReceiver("pip-int-powel-smgexmespull-in", ReceiveMode.PeekLock);
            
            while (!_busReceiver.IsClosed && !_cancelSource.Token.IsCancellationRequested)
            {
                var nextMsg = GetNextBrokeredMessage();
                if (nextMsg != null)
                {
                    KeepAliveAndProcessMessage(nextMsg, _cancelSource.Token);
                }
                //Task.Run(()=>Thread.Sleep(TestRunSettings.PollInterval), _cancelSource.Token);
            }
        }

        private void KeepAliveAndProcessMessage(BrokeredMessage nextMsg, CancellationToken token)
        {
            if (token.IsCancellationRequested)
                return;
            var workTask = Task.Run(() => DoWithMessage(nextMsg, _cancelSource.Token),token);
            
            var expTime = DateTime.SpecifyKind(nextMsg.LockedUntilUtc, DateTimeKind.Utc);
            var expPeriod = expTime.Subtract(DateTime.UtcNow);
            var t = SetUpRenewLockTimer(nextMsg, expPeriod);

            /*Wait for work completion or cancellation:*/
            try
            {
                workTask.Wait(token);
            }
            catch (OperationCanceledException) { /*To be expected...*/ }

            /*Kill timer...*/
            t.Stop();
            t.Dispose();
        }

        private Timer SetUpRenewLockTimer(BrokeredMessage nextMsg, TimeSpan expPeriod)
        {
            var t = new Timer
            {
                Interval = expPeriod.Add(TimeSpan.FromSeconds(-10)).TotalMilliseconds,
                AutoReset = true
            };
            t.Start();
            t.Elapsed += (o, e) => RenewTheLock(nextMsg);
            return t;
        }

        private void RenewTheLock(BrokeredMessage message)
        {
            try
            {
                message.RenewLock();
                LogMessage(message.MessageId,
                    $"Renewed lock, now valid until: {DateTime.SpecifyKind(message.LockedUntilUtc, DateTimeKind.Utc).ToLocalTime().ToString("HH:mm:ss")}");
            }
            catch (Exception e)
            {
                LogMessage(message.MessageId, $"Lock renew failed! ({e.GetType().Name}: {e.Message})");
                throw;
            }
            
        }

        private BrokeredMessage GetNextBrokeredMessage()
        {
            BrokeredMessage nextMsg = null;
            /*Following code does not work; Messages need to be defered before they can be received by sequence# */
            /*for (int i = 0; i < 10; i++)
            {
                BrokeredMessage msg = _busReceiver.Peek(_busReceiver.LastPeekedSequenceNumber + 1);
                if (msg != null)
                {
                    if (nextMsg == null)
                    {
                        nextMsg = msg;
                    }
                    if ((int) GetPriority(nextMsg) > (int) GetPriority(msg))
                    {
                        nextMsg = msg;
                    }
                    continue;
                }
                break;
            }*/
            nextMsg = _busReceiver.Receive(TestRunSettings.PollInterval);
            return nextMsg;
        }

        private DataExchangeQueuePriority GetPriority(BrokeredMessage metaData, string payload = null)
        {
            object obj;
            return metaData.Properties.TryGetValue("Priority", out obj) 
                ? DataExchangeQueuePriorityConverter.FromString(obj.ToString()) 
                : DataExchangeQueuePriority.Normal;
        }
    }

    

}
