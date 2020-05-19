# MongoDbStorage

appsettings.json

  "MongodbConfig": {
    "CollectionName": "collectionname",
    "ConnectionString": "con string",
    "DbName": "DbName"
  }
  
  Startup->ConfigureServices
  
            services.Configure<MongodbConfig>(Configuration.GetSection(nameof(MongodbConfig)));
            services.AddSingleton<IMongodbConfig>(sp =>sp.GetRequiredService<IOptions<MongodbConfig>>().Value);
            services.AddSingleton<IStorage, MongoDbStorage>();
            services.AddSingleton<UserState>(); 
            services.AddSingleton<ConversationState>();
