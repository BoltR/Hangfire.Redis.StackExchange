using Hangfire.States;
using Hangfire.Storage;
using NSubstitute;
using System;
using System.Reflection;
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
            _context.Job.Id = JobId;
            _context.NewStateValue = CreateSucceededState();
            _transaction = Substitute.For<IWriteOnlyTransaction>();
        }

        // SucceededState has been made internal in Hangfire.Core
        private SucceededState CreateSucceededState()
        {
            return (SucceededState)typeof(SucceededState).GetConstructor(
                  BindingFlags.NonPublic | BindingFlags.Instance,
                  null, new Type[] { typeof(object), typeof(long), typeof(long) }, null).Invoke(new object[] { null, 11, 223 });
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
