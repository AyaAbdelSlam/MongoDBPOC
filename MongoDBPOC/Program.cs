using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MongoDBPOC
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var client = new MongoClient("mongodb+srv://ahemaid:AYOUSHA_sta_123@cluster0.diaxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority");
            var database = client.GetDatabase("sample_training");
            var collection = database.GetCollection<BsonDocument>("grades");

            #region List all Db

            //// List all Db in this connection
            //var dbList = client.ListDatabases().ToList();
            //Console.WriteLine("The list of databases on this server is: ");
            //foreach (var db in dbList)
            //{
            //    Console.WriteLine(db);
            //}
            #endregion


            #region non-async operations insert / insertmany / read
            // Create new instance to add it in this collection
            var document = new BsonDocument
            {
                { "student_id", 10000 },
                { "scores", new BsonArray
                    {
                    new BsonDocument{ {"type", "exam"}, {"score", 88.12334193287023 } },
                    new BsonDocument{ {"type", "quiz"}, {"score", 74.92381029342834 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 89.97929384290324 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 82.12931030513218 } }
                    }
                },
                { "class_id", 480}
            };
            collection.InsertOne(document);

            // inset many items 
            //collection.InsertMany(new List<BsonDocument>() { document });

            //Reading only one Document
            var firstDocument = collection.Find(new BsonDocument()).FirstOrDefault();
            Console.WriteLine(firstDocument.ToString());

            //Read all Documents 
            var documents = collection.Find(new BsonDocument()).ToList();

            #region Filters
            var filter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);

            var studentDocument = collection.Find(filter).FirstOrDefault();
            Console.WriteLine(studentDocument.ToString());

            var highExamScoreFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
            "scores", new BsonDocument { { "type", "exam" },
            { "score", new BsonDocument { { "$gte", 95 } } }
            });

            var highExamScores = collection.Find(highExamScoreFilter).ToList();
            #endregion

            #endregion



            #region Async Operations inser / insertmany / read
            // async 
            //await collection.InsertOneAsync(document);

            //insert many instances at once / async
            //collection.InsertMany() / collection.InsertManyAsync()
            #endregion


            #region CRUD Insert / find / delete / update

            //insert
            var document2 = new BsonDocument
            {
                { "student_id", 10000 },
                { "scores", new BsonArray
                    {
                    new BsonDocument{ {"type", "exam"}, {"score", 88.12334193287023 } },
                    new BsonDocument{ {"type", "quiz"}, {"score", 74.92381029342834 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 89.97929384290324 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 82.12931030513218 } }
                    }
                },
                { "class_id", 480}
            };
            collection.InsertOne(document);

            // find
            var firstDocument2 = collection.Find(new BsonDocument()).FirstOrDefault();
            Console.WriteLine(firstDocument.ToString());

            //update

            //var document = new BsonDocument
            //{
            //    { "student_id", 10000 },
            //    { "scores", new BsonArray
            //        {
            //        new BsonDocument{ {"type", "exam"}, {"score", 88.12334193287023 } },
            //        new BsonDocument{ {"type", "quiz"}, {"score", 74.92381029342834 } },
            //        new BsonDocument{ {"type", "homework"}, {"score", 89.97929384290324 } },
            //        new BsonDocument{ {"type", "homework"}, {"score", 82.12931030513218 } }
            //        }
            //    },
            //    { "class_id", 480}
            //};

            var update = Builders<BsonDocument>.Update.Set("class_id", 483);

            var filterUpdate = Builders<BsonDocument>.Filter.Eq("student_id", 10000);
            collection.UpdateOne(filterUpdate, update);

            var arrayFilter = Builders<BsonDocument>.Filter.Eq("student_id", 10000) & Builders<BsonDocument>
                  .Filter.Eq("scores.type", "quiz");
            var arrayUpdate = Builders<BsonDocument>.Update.Set("scores.$.score", 84.92381029342834);

            collection.UpdateOne(arrayFilter, arrayUpdate);

            //delete
            var deleteFilter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);
            collection.DeleteOne(deleteFilter);


            //Multi Delete
            var deleteLowExamFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>("scores",
     new BsonDocument { { "type", "exam" }, {"score", new BsonDocument { { "$lt", 60 }}}
});
            collection.DeleteMany(deleteLowExamFilter);



            #endregion

            #region for Iteration over documents on the fly

            //var cursor = collection.Find(highExamScoreFilter).ToCursor();
            //foreach (var document in cursor.ToEnumerable())
            //{
            //    Console.WriteLine(document);
            //}

            ////Async 
            //await collection.Find(highExamScoreFilter).ForEachAsync(document => Console.WriteLine(document));
            #endregion


            #region Sorting

            var sort = Builders<BsonDocument>.Sort.Descending("student_id");
            var highestScores = collection.Find(highExamScoreFilter).Sort(sort);

            // getting the first student after sorting
            var highestScore = collection.Find(highExamScoreFilter).Sort(sort).First();
            Console.WriteLine(highestScore);
            #endregion

            #region Aggregation Pipline in C#

            // Various stages in pipeline are 

            //$project select, reshape data
            //$match filter data
            //$group aggregate data
            //$sort sorts data
            //$skip skips data
            //$limit limit data
            //$unwind normalizes data

            //            $project
            //In $project phase, we can add a key, remove a key, reshape a key. There are also some simple functions that we can use on the key: $toUpper, $toLower, $add, $multiply etc.

    //        var dataFacet = AggregateFacet.Create("data",
    //PipelineDefinition<Person, Person>.Create(new[]
    //{
    //    PipelineStageDefinitionBuilder.Sort(Builders<Person>.Sort.Ascending(x => x.Surname)),
    //    PipelineStageDefinitionBuilder.Skip<Person>((page - 1) * pageSize),
    //    PipelineStageDefinitionBuilder.Limit<Person>(pageSize),
    //}));

    //        var filter = Builders<Person>.Filter.Empty;
    //        var aggregation = await collection.Aggregate()
    //            .Match(filter)
    //            .Facet(countFacet, dataFacet)
    //            .ToListAsync();

    //        var count = aggregation.First()
    //            .Facets.First(x => x.Name == "count")
    //            .Output<AggregateCountResult>()
    //            ?.FirstOrDefault()
    //            ?.Count ?? 0;

    //        var totalPages = (int)count / pageSize;

    //        var data = aggregation.First()
    //            .Facets.First(x => x.Name == "data")
    //            .Output<Person>();
            #endregion
        }
    }
}
