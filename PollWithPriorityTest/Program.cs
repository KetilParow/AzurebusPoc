using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Configuration;
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
                Task t = Task.Run(() => new MultipleTopicPollReceiver(cancelSource), cancelSource.Token);                
                WaitForCancellation(cancelSource);
                if (t.Status == TaskStatus.Running && cancelSource.Token.IsCancellationRequested)
                {
                    try { t.Wait(cancelSource.Token);} catch(OperationCanceledException) { };
                }
            }
            ShowEndMessage();
        }
    }
    internal class PriorityListener
    {
        public DataExchangeQueuePriority Priority { get; set; }
        public string SubsciptionName { get; set; }
        public string Topic { get; set; }
        public SubscriptionClient Client { get; set; }
    }

    internal class MultipleTopicPollReceiver : BrokeredMessageBaseHandler
    {
        private IList<PriorityListener> _listeners;
        public MultipleTopicPollReceiver(CancellationTokenSource cancelSource) :base(cancelSource, TestRunSettings.CompleteMessageAfterTime, TestRunSettings.Logger)
        {
            ReceiveMessages();
        }

        private void ReceiveMessages()
        {
            _messagingFactory = MessagingFactory.CreateFromConnectionString(ConsoleTest.AzbusConnectionString);
            var client = _messagingFactory.CreateSubscriptionClient("", "", ReceiveMode.PeekLock);
            _busReceiver = _messagingFactory.CreateMessageReceiver(ConsoleTest.AzbusEntityPath, ReceiveMode.PeekLock);
            _listeners = GetListeners().ToList();

            while (!_busReceiver.IsClosed && !_cancelSource.Token.IsCancellationRequested)
            {
                var nextMsg = GetNextMessage();
                if (nextMsg != null)
                {
                    KeepAliveAndProcessMessage(nextMsg, _cancelSource.Token);
                }
                Task.Run(() => Thread.Sleep(TestRunSettings.PollInterval), _cancelSource.Token);
            }
        }

        private BrokeredMessage GetNextMessage()
        {
            BrokeredMessage msg = null;
            foreach (var listener in _listeners)
            {
                msg = listener.Client.Receive();
                if (msg != null)
                {
                    break;
                }
            }
            return msg;
        }

        private IEnumerable<PriorityListener> GetListeners()
        {
            return Enum.GetNames(typeof(DataExchangeQueuePriority)).Select(ps => (DataExchangeQueuePriority) Enum.Parse(typeof(DataExchangeQueuePriority), ps)).OrderBy(p=>(int)p).Select(
                p => new PriorityListener
            {
                Client = _messagingFactory.CreateSubscriptionClient("", "", ReceiveMode.PeekLock),
                Priority = p,
                Topic = p.ToString(),
                SubsciptionName = "TheSubscriptionName"
            });
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

        //private DataExchangeQueuePriority GetPriority(BrokeredMessage metaData, string payload = null)
        //{
        //    object obj;
        //    return metaData.Properties.TryGetValue("Priority", out obj) 
        //        ? DataExchangeQueuePriorityConverter.FromString(obj.ToString()) 
        //        : DataExchangeQueuePriority.Normal;
        //}
    }

}
