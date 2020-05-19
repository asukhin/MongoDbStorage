using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using Microsoft.Bot.Builder;
using Microsoft.Bot.Schema;
using System.Threading;

namespace Microsoft.Bot.Builder.MongoDb
{
    public class MongoDbService
    {

        private readonly IMongoCollection<BotStateEntity> mongoCollection;
        public MongoDbService(IMongodbConfig mongodbConfig)
        {
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogInstance>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogState>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.ActivityPrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.AttachmentPrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.BeginSkillDialogOptions>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.ChoicePrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.ComponentDialog>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.ConfirmPrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DateTimePrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DateTimeResolution>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.Dialog>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogContainer>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogContext>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogContextVisibleState>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogEvent>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogEvents>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogManagerResult>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogPath>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogSet>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.DialogTurnResult>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.OAuthPrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.OAuthPromptSettings>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.PersistedState>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.PersistedStateKeys>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.PromptOptions>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.Recognizer>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.SkillDialog>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.SkillDialogOptions>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.TextPrompt>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.ThisPath>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.TurnPath>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.WaterfallDialog>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.Dialogs.WaterfallStepContext>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.UserState>();
            BsonClassMap.RegisterClassMap<Microsoft.Bot.Builder.ConversationState>();
            var cleint = new MongoClient(mongodbConfig.ConnectionString);
            var db = cleint.GetDatabase(mongodbConfig.DbName);
            mongoCollection = db.GetCollection<BotStateEntity>(mongodbConfig.CollectionName);

        }
        /// <summary>
        /// Get data from db
        /// </summary>
        public List<BotStateEntity> Get() => mongoCollection.Find<BotStateEntity>(x => true).ToList();
        public List<BotStateEntity> GetMany(string[] keys)
        {
            List<BotStateEntity> botStateEntities = new List<BotStateEntity> { };
            foreach (string key in keys)
            {
                var list = mongoCollection.Find<BotStateEntity>(x => x.Key == key).ToList();
                botStateEntities.AddRange(list);
            }
            return botStateEntities;
        }
        /// <summary>
        /// Get data from db by key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public BotStateEntity Get(string key) => mongoCollection.Find<BotStateEntity>(x => x.Key == key).FirstOrDefault();
        /// <summary>
        /// Create new 
        /// </summary>
        /// <param name="botStateEntity"></param>
        /// <returns></returns>
        public BotStateEntity Create(BotStateEntity botStateEntity)
        {
            lock (mongoCollection)
            {
                mongoCollection.InsertOne(botStateEntity);
            }
            return botStateEntity;

        }
        /// <summary>
        /// Update 
        /// </summary>
        /// <param name="key">Key for select</param>
        /// <param name="botStateEntity">object for replace</param>
        /// <param name="isUpsert">inser new</param>
        public void Update(string key, BotStateEntity botStateEntity, bool isUpsert = true) { lock (mongoCollection) { mongoCollection.ReplaceOne(new BsonDocument("Key", $"{key}"), botStateEntity, new ReplaceOptions() { IsUpsert = isUpsert }); } }
        /// <summary>
        /// Delete record
        /// </summary>
        /// <param name="key"></param>
        public void DeleteOne(string key)
        {
            lock (mongoCollection)
            {
                mongoCollection.DeleteMany(x => x.Key == key);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public void DeleteMany(string[] keys)
        {

            lock (mongoCollection)
            {
                foreach (string key in keys)
                {
                    this.DeleteOne(key);
                }

            }

        }

    }

    public interface IMongodbConfig
    {

        String CollectionName { get; set; }
        String ConnectionString { get; set; }
        String DbName { get; set; }
    }

    public class MongodbConfig : IMongodbConfig
    {
        public string CollectionName { get; set; }
        public string ConnectionString { get; set; }
        public string DbName { get; set; }
    }


    public class BotStateEntity
    {

        [BsonId]
        //[BsonRepresentation(BsonType.ObjectId)]
        public ObjectId Id { get; set; }

        public String Key { get; set; }

        public Object ObjectState { get; set; }
    }
    /// <summary>
    /// 
    /// </summary>
    public class MongoDbStorage : MongoDbService, IStorage
    {

        public MongoDbStorage(IMongodbConfig mongodbConfig) : base(mongodbConfig) { }


        #region Implementation IStorage
        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task DeleteAsync(string[] keys, CancellationToken cancellationToken = default)
        {
            return DeleteRecordsAsync(keys);
        }
        /// <summary>
        /// read record from db
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<IDictionary<string, object>> ReadAsync(string[] keys, CancellationToken cancellationToken = default)
        {
            return ReadAsync(keys);
        }
        /// <summary>
        /// Write changes into db
        /// </summary>
        /// <param name="changes"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task WriteAsync(IDictionary<string, object> changes, CancellationToken cancellationToken = default)
        {
            return WriteAsync(changes);
        }

        #endregion
        #region Delete 
        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        private void DeleteRecords(string[] keys)
        {

            base.DeleteMany(keys);

        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        private async Task DeleteRecordsAsync(string[] keys)
        {


            await Task.Run(() => DeleteRecords(keys));
        }
        #endregion
        #region Read

        private IDictionary<string, Object> Read(string[] keys)
        {

            if (keys.Length > 0)
            {

                Dictionary<String, object> keyValuePairs = new Dictionary<String, Object> { };
                foreach (string key in keys)
                {
                    keyValuePairs.Add(key, null);

                }

                var list = base.GetMany(keys);

                if (list.Count > 0)
                {
                    foreach (BotStateEntity botState in list)
                    {

#if DEBUG
                        System.Diagnostics.Debug.WriteLine($"Read {botState.Key}");
#endif
                        if (!keyValuePairs.ContainsKey(botState.Key))
                            continue;

                        keyValuePairs[botState.Key] = botState.ObjectState;


                    }
                    return keyValuePairs;

                }
                return keyValuePairs;

            }

            return null;
        }

        private async Task<IDictionary<string, Object>> ReadAsync(string[] keys)
        {

            return await Task.Run(() => Read(keys));

        }


        #endregion
        #region Write

        private void Write(IDictionary<string, object> changes)
        {

            if (changes != null && changes.Count > 0)
            {

                foreach (string key in changes.Keys)
                {

#if DEBUG
                    System.Diagnostics.Debug.WriteLine($"Write {key}");
#endif
                    var field = base.Get(key);

                    if (field == null)
                    {
                        base.Update(key, new BotStateEntity()
                        {

                            //Id=key,
                            Id = ObjectId.GenerateNewId(),
                            Key = key,
                            ObjectState = changes[key]

                        }, true);
                    }
                    else
                    {
                        base.Update(key, new BotStateEntity()
                        {
                            Id = field.Id,
                            Key = key,
                            ObjectState = changes[key]
                        }, true);


                    }

                }
            }

        }

        private async Task WriteAsync(IDictionary<string, object> changes)
        {

            await Task.Run(() => Write(changes));
        }
        #endregion

    }
}
