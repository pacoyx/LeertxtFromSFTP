using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace AppLeerInputs.NoSql
{
    public class MongoHelper
    {
        public static IMongoClient client { get; set; }
        public static IMongoDatabase database { get; set; }

        public static string MongoConnection = "mongodb+srv://mongosa:LR6bgHNxJbJsWxei@cluster0-0cggd.mongodb.net/test?retryWrites=true&w=majority";
        public static string MongoDatabase = "settleDB";

        public static IMongoCollection<Models.In_gp_comercios_txtBE> comercios_collection { get; set; }
        public static IMongoCollection<Models.In_gp_trx_txtBE> trxs_collection { get; set; }

        internal static void ConnectToMongoService()
        {
            try
            {
                client = new MongoClient(MongoConnection);
                database = client.GetDatabase(MongoDatabase);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex);
            }

        }

    }
}
