using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Redis.StackExchange
{
    internal static class RedisExtensionMethods
    {
        public static HashEntry[] ToHashEntryArray(this IEnumerable<KeyValuePair<string, string>> Items)
        {
            var Entries = new HashEntry[Items.Count()];
            int i = 0;
            foreach (var Item in Items)
            {
                Entries[i++] = new HashEntry(Item.Key, Item.Value);
            }
            return Entries;
        }

        public static HashEntry[] ToHashEntryArray(this Dictionary<string, string> Items)
        {
            var Entries = new HashEntry[Items.Count];
            int i = 0;
            foreach (var Item in Items)
            {
                Entries[i++] = new HashEntry(Item.Key, Item.Value);
            }
            return Entries;
        }

        public static List<string> ToStringList(this RedisValue[] Values)
        {
            var Result = new List<string>();
            foreach (var item in Values)
            {
                Result.Add(item);
            }
            return Result;
        }
    }
}
