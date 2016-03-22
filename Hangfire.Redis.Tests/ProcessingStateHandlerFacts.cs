using Hangfire.States;
using NSubstitute;
using System;
using System.Reflection;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class ProcessingStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;

        public ProcessingStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();
            _context.Job.Id = JobId;
            _context.NewStateValue = CreateProcessingState();
        }

        // ProcessingState has been made internal in Hangfire.Core
        private ProcessingState CreateProcessingState()
        {
            return (ProcessingState)typeof(ProcessingState).GetConstructor(
                  BindingFlags.NonPublic | BindingFlags.Instance,
                  null, new Type[] { typeof(string), typeof(string) }, null).Invoke(new object[] { "server", "1" });
        }

        [Fact]
        public void StateName_ShouldBeEqualToProcessingState()
        {
            var handler = new ProcessingStateHandler();
            Assert.Equal(ProcessingState.StateName, handler.StateName);
        }

        [Fact]
        public void Apply_ShouldAddTheJob_ToTheProcessingSet()
        {
            var handler = new ProcessingStateHandler();
            handler.Apply(_context.Object, _context.Transaction);

            _context.Transaction.Received().AddToSet("processing", JobId, Arg.Any<double>());
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheProcessingSet()
        {
            var handler = new ProcessingStateHandler();
            handler.Unapply(_context.Object, _context.Transaction);

            _context.Transaction.Received().RemoveFromSet("processing", JobId);
        }
    }
}
