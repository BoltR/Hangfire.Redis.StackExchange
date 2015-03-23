using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;
using Xunit;

namespace Hangfire.Redis.StackExchange.Tests
{
    public class SucceededStateHandlerFacts
    {
        private const string JobId = "1";

        private readonly ApplyStateContextMock _context;
        private readonly IWriteOnlyTransaction _transaction;
        
        public SucceededStateHandlerFacts()
        {
            _context = new ApplyStateContextMock();
            _context.StateContextValue.JobIdValue = JobId;
            _context.NewStateValue = new SucceededState(null, 11, 123);

            _transaction = Substitute.For<IWriteOnlyTransaction>();
        }

        [Fact]
        public void StateName_ShouldBeEqualToSucceededState()
        {
            var handler = new SucceededStateHandler();
            Assert.Equal(SucceededState.StateName, handler.StateName);
        }

        [Fact]
        public void Apply_ShouldInsertTheJob_ToTheBeginningOfTheSucceededList_AndTrimIt()
        {
            var handler = new SucceededStateHandler();
            handler.Apply(_context.Object, _transaction);

            _transaction.Received().InsertToList("succeeded", JobId);
            _transaction.Received().TrimList("succeeded", 0, 99);
        }

        [Fact]
        public void Unapply_ShouldRemoveTheJob_FromTheSucceededList()
        {
            var handler = new SucceededStateHandler();
            handler.Unapply(_context.Object, _transaction);

            _transaction.Received().RemoveFromList("succeeded", JobId);
        }
    }
}
