﻿// Copyright © 2013-2014 Sergey Odinokov.
// Copyright © 2015 Daniel Chernis.
//
// Hangfire.Redis.StackExchange is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Redis.StackExchange is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Redis.StackExchange. If not, see <http://www.gnu.org/licenses/>.

using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using System;

namespace Hangfire.Redis.StackExchange
{
    internal class FailedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.AddToSet(
                "failed",
                context.JobId,
                JobHelper.ToTimestamp(DateTime.UtcNow));
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromSet("failed", context.JobId);
        }

        public string StateName
        {
            get { return FailedState.StateName; }
        }
    }
}
