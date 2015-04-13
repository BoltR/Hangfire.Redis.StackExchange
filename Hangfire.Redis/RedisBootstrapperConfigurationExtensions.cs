// Copyright © 2013-2014 Sergey Odinokov.
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

using StackExchange.Redis;
using System;

namespace Hangfire.Redis.StackExchange
{
    public static class RedisBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at localhost:6379 and use the '0' db to store 
        /// the data.
        /// </summary>
        [Obsolete("Please use `GlobalConfiguration.UseRedisStorage` instead. Will be removed in version 2.0.0.")]
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration)
        {
            var storage = new RedisStorage();
            configuration.UseStorage(storage);
            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port and store the
        /// data in db with number '0'.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="OptionString">StackExchange.Redis option string</param>
        [Obsolete("Please use `GlobalConfiguration.UseRedisStorage` instead. Will be removed in version 2.0.0.")]
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            string OptionString)
        {
            var storage = new RedisStorage(OptionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port, and store the
        /// data in the given database number.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="OptionString">StackExchange.Redis option string</param>
        /// <param name="db">Database number to store the data, for example '0'</param>
        /// <returns></returns>
        [Obsolete("Please use `GlobalConfiguration.UseRedisStorage` instead. Will be removed in version 2.0.0.")]
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            string OptionString,
            int db)
        {
            var storage = new RedisStorage(OptionString, db);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port, and store the
        /// data in the given database number.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="Options">StackExchange ConfigurationOptions</param>
        /// <returns></returns>
        [Obsolete("Please use `GlobalConfiguration.UseRedisStorage` instead. Will be removed in version 2.0.0.")]
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            ConfigurationOptions Options)
        {
            var storage = new RedisStorage(Options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port, and store the
        /// data in the given database number.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="Options">StackExchange ConfigurationOptions</param>
        /// <param name="db">Database number to store the data, for example '0'</param>
        /// <returns></returns>
        [Obsolete("Please use `GlobalConfiguration.UseRedisStorage` instead. Will be removed in version 2.0.0.")]
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            ConfigurationOptions Options,
            int db)
        {
            var storage = new RedisStorage(Options, db);
            configuration.UseStorage(storage);

            return storage;
        }
    }
}
