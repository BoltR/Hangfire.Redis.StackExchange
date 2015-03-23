using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class ProcessingStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly IWriteOnlyTransaction _transaction;
        
        public ProcessingStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();
            _context.StateContextValue.JobIdValue = JobId;
            _context.NewStateValue = new ProcessingState("server", 1);

            _transaction = Substitute.For<IWriteOnlyTransaction>();
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
            handler.Apply(_context.Object, _transaction);

            _transaction.Received().AddToSet("processing", JobId, Arg.Any<double>());
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheProcessingSet()
        {
            var handler = new ProcessingStateHandler();
            handler.Unapply(_context.Object, _transaction);

            _transaction.Received().RemoveFromSet("processing", JobId);
        }
    }
}
