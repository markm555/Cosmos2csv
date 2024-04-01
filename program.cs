using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
//using CsvHelper;


class Program
{
    private static readonly string EndpointUrl = "<Your Cosmos DB URL>";
    private static readonly string AuthorizationKey = "<Your Key>";
    private static readonly string DatabaseId = "<Your Database>";
    private static readonly string ContainerId = "<Your Container>";
    private static CosmosClient cosmosClient;

    static async Task Main(string[] args)
    {
        cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey);
        var container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        var sqlQueryText = "SELECT * FROM c OFFSET 0 LIMIT 20000000";
        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
        FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
        DateTime StartTime = DateTime.Now;
        Console.WriteLine(StartTime.ToString());
        // Change the following line to point to the location you want your CSV file to be written.  double \\ are required
        using (var writer = new StreamWriter("C:\\Data\\cosmosdb.csv"))
        //using (var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
        {
            bool headersWritten = false;

            while (queryResultSetIterator.HasMoreResults)
            {
                //FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                //foreach (var item in currentResultSet)
                //{
                //    Console.WriteLine(item);
                //}

                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();

                foreach (var item in currentResultSet)
                {
                    JObject json = JObject.Parse(item.ToString());

                    if (!headersWritten)
                    {
                        foreach (var prop in json.Properties())
                        {
                            writer.Write($"{prop.Name},");
                        }
                        writer.WriteLine();
                        headersWritten = true;
                    }

                    foreach (var prop in json.Properties())
                    {
                        writer.Write($"{prop.Value},");
                        //Console.WriteLine(prop.Value);
                    }
                    writer.WriteLine();
                }
            }
            DateTime EndTime = DateTime.Now;
            Console.WriteLine(EndTime.ToString());
            Console.WriteLine("Elapsed Time: " + (EndTime - StartTime));
        }
    }
}
