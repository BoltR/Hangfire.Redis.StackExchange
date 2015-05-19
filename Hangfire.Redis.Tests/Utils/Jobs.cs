using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.Redis.StackExchange.Tests
{
	public class Jobs
	{
		public void Default()
		{
		}

		[Queue("critical")]
		public void Critical()
		{
		}
	}
}
