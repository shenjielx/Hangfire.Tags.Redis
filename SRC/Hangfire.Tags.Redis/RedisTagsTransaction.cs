using System;
using Hangfire.Annotations;
using Hangfire.Redis;
using Hangfire.Storage;
using Hangfire.Tags.Storage;
using StackExchange.Redis;

namespace Hangfire.Tags.Redis
{
    internal class RedisTagsTransaction : ITagsTransaction
    {
        private readonly RedisStorageOptions _options;

        private readonly IDatabase _database;

        public RedisTagsTransaction(RedisStorageOptions options, IWriteOnlyTransaction transaction, IDatabase database)
        {
            if (options.UseTransactions && transaction.GetType().Name != "RedisWriteOnlyTransaction")
                throw new ArgumentException("The transaction is not an Redis transaction", nameof(transaction));

            _database = database;
            _options = options;
        }

        public void ExpireSetValue(string key, string value, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            _database.KeyExpire(GetRedisKey(key), expireIn);
        }

        public void PersistSetValue(string key, string value)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            _database.KeyPersist(GetRedisKey(key));
        }

        internal string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _options.Prefix + key;
        }

    }
}
