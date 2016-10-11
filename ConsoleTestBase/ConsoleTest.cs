using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace ConsoleTestBase
{
    public abstract class ConsoleTest //where T: IDisposable, new()
    {
        public const string AzbusConnectionString = "Endpoint=sb://pip-sb-eonnor-dev.servicebus.windows.net/;SharedAccessKeyName=smgexmespull;SharedAccessKey=DzNElW9G2tH1895uRIOeccm/HRGvN2/JecJHhqlKAck=";
        public const string AzbusEntityPath = "pip-int-powel-smgexmespull-in";

        protected static void WaitForCancellation(CancellationTokenSource cancelSource)
        {
            ShowBestMagnificentMessage();
            var quit = false;
            while (!quit)
            {
                var pressed = Console.ReadKey();
                if (pressed.KeyChar == 'q' || pressed.KeyChar == 'Q')
                {
                    Console.WriteLine("");
                    cancelSource.Cancel();
                    quit = true;
                    continue;
                }
                ShowBestMagnificentMessage();
            }
        }

        private static void ShowBestMagnificentMessage()
        {
            Console.WriteLine("\nHit letter 'q' to exit");
        }

        protected static void ShowEndMessage()
        {
            Console.Write("\nAll cleaned up. Hit any key");
            Console.ReadKey(true);
        }
    }

    public abstract class BrokeredMessageBaseHandler: IDisposable
    {
        protected MessageReceiver _busReceiver;
        protected MessagingFactory _messagingFactory;
        protected readonly CancellationTokenSource _cancelSource;
        private readonly Action<string> _logger;
        private readonly TimeSpan? _completeMessageAfterTime;

        public BrokeredMessageBaseHandler(CancellationTokenSource cancelSource, TimeSpan? completeMessageAfterTime, Action<string> logger)
        {
            _cancelSource = cancelSource;
            _completeMessageAfterTime = completeMessageAfterTime;
            _logger = logger;
        }
        
        protected bool DoWithMessage(BrokeredMessage obj, CancellationToken cToken)
        {
            var expTime = DateTime.SpecifyKind(obj.LockedUntilUtc, DateTimeKind.Utc);
            var wasCompleted = false;
            LogMessage(obj.MessageId, $"Received, locked until: {expTime.ToLocalTime().ToString("HH:mm:ss")}, Delivery# {obj.DeliveryCount}");
            try
            {
                var worker =
                    Task.Run(() => { wasCompleted = NeverFinish(obj, expTime.Subtract(DateTime.UtcNow), DateTime.Now, cToken); },
                        cToken);
                worker.Wait(cToken);
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
                return false;
            }
            return wasCompleted;
        }
        
        private bool NeverFinish(BrokeredMessage obj, TimeSpan expPeriod, DateTime startDate, CancellationToken cToken)
        {
            string id = obj.MessageId;
            while (!cToken.IsCancellationRequested)
            {
                DoSomeWorkThatExceedsCurrentLock(expPeriod.Add(TimeSpan.FromSeconds(2)), cToken);
                if (cToken.IsCancellationRequested)
                {
                    return false;
                }
                if (MessageWasCompleted(obj, startDate))
                {
                    return true;
                }
                LogMessage(id, $"Time alive {DateTime.Now.Subtract(startDate)}, now locked until: {DateTime.SpecifyKind(obj.LockedUntilUtc, DateTimeKind.Utc).ToLocalTime().ToString("HH:mm:ss")}, Delivery# {obj.DeliveryCount}");
            }
            return false;
        }

        private bool MessageWasCompleted(BrokeredMessage obj, DateTime startDate)
        {
            if (!CompleteMessageAfterTime.HasValue)
            {
                return false;
            }
            if (DateTime.Now.Subtract(startDate) > CompleteMessageAfterTime.Value)
            {
                try
                {
                    LogMessage(obj.MessageId, $"Completing after {CompleteMessageAfterTime.Value}");
                    obj.Complete();
                    return true;
                }
                catch (Exception e)
                {
                    LogMessage(obj.MessageId, $"Error: {e.Message}");
                    throw;
                }
            }
            return false;
        }

        
        protected void LogMessage(string id, string message)
        {
            Logger($"\n{id},{DateTime.Now.ToString("HH:mm:ss")},Thread#{Thread.CurrentThread.ManagedThreadId}: {message}");
        }

        private static void DoSomeWorkThatExceedsCurrentLock(TimeSpan sleepSpan, CancellationToken cToken)
        {
            if (cToken.IsCancellationRequested) return;
            try
            {
                var t = Task.Run(() => Thread.Sleep(sleepSpan), cToken);
                t.Wait(cToken);
            }
            catch (OperationCanceledException) { /*Should be expected*/ }

        }

        public virtual void Dispose()
        {
            if (!_busReceiver.IsClosed)
            {
                _busReceiver.Abort();
                _busReceiver.Close();
            }

            if (!_messagingFactory.IsClosed)
            {
                _messagingFactory.Close();
            }
        }

        protected Action<string> Logger
        {
            get
            {
                if (_logger == null)
                    return Console.WriteLine;
                return _logger;
            }
        }

        private TimeSpan? CompleteMessageAfterTime
        {
            get { return _completeMessageAfterTime; }
        }
    }
    public enum DataExchangeQueuePriority
    {
        High,
        Normal,
        Low,
        Undefined
    }
    public static class DataExchangeQueuePriorityConverter
    {
        public static DataExchangeQueuePriority FromString(string value)
        {
            DataExchangeQueuePriority priority;

            if (!Enum.TryParse(value, true, out priority))
            {
                priority = DataExchangeQueuePriority.Undefined;
            }

            return priority;
        }
    }
}
