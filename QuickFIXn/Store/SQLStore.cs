using Microsoft.Data.SqlClient;
using QuickFix.Store;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickFix
{
    public class SQLStore : IMessageStore
    {
        private MemoryStore cache_ = new MemoryStore();

        private SessionID _sessionID;
        private SessionSettings _sessionSettings;

        private string messages_table = "messages";
        private string sessions_table = "sessions";
        private string _connectionString = string.Empty;
        private string _user = string.Empty;
        private string _pwd = string.Empty;
        private string _datasource = string.Empty;
        private string _initialcatalog = string.Empty;

        public SQLStore(SessionID sessionId, string user, string password, string connectionString, SessionSettings settings)
        {
            _sessionID = sessionId;
            _sessionSettings = settings;

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_SESSION_TABLE))
                sessions_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_SESSION_TABLE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_MESSAGES_TABLE))
                messages_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_MESSAGES_TABLE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_DATASOURCE))
                _datasource = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_DATASOURCE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_INITIAL_CATALOG))
                _initialcatalog = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_INITIAL_CATALOG);

            _connectionString = connectionString;
            _user = user;
            _pwd = password;

            PopulateCache();
        }

        /// <summary>  
        /// You must edit the four 'my' string values.  
        /// </summary>  
        /// <returns>An ADO.NET connection string.</returns>  
        private string GetSqlConnectionString()
        {
            // Prepare the connection string to Azure SQL Database.  
            var sqlConnectionSB = new SqlConnectionStringBuilder();


            if (!string.IsNullOrEmpty(_connectionString))
            {
                sqlConnectionSB.ConnectionString = _connectionString;
            }
            else
            {
                // Change these values to your values.  
                sqlConnectionSB.DataSource = _datasource; // "tcp:myazuresqldbserver.database.windows.net,1433"; //["Server"]  
                sqlConnectionSB.InitialCatalog = _initialcatalog; // "MyDatabase"; //["Database"]  

                if (!string.IsNullOrEmpty(_user) && !string.IsNullOrEmpty(_pwd))
                {
                    sqlConnectionSB.UserID = _user;  // "@yourservername"  as suffix sometimes.  
                    sqlConnectionSB.Password = _pwd;
                    // Leave these values as they are.  
                    sqlConnectionSB.IntegratedSecurity = false;
                }
                else
                {
                    sqlConnectionSB.IntegratedSecurity = true;
                }

            }

            sqlConnectionSB.Encrypt = true;
            sqlConnectionSB.ConnectTimeout = 30;
            sqlConnectionSB.TrustServerCertificate = true;

            // Adjust these values if you like. (ADO.NET 4.5.1 or later.)  
            sqlConnectionSB.ConnectRetryCount = 5;
            sqlConnectionSB.ConnectRetryInterval = 5;  // Seconds.  


            return sqlConnectionSB.ToString();
        }


        public void PopulateCache()
        {
            string queryString = string.Empty;

            queryString = "SELECT creation_time, incoming_seqnum, outgoing_seqnum FROM " + sessions_table + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";



            using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
            {
                using (var dbCommand = sqlConnection.CreateCommand())
                {
                    dbCommand.CommandText = queryString;

                    sqlConnection.Open();
                    var reader = dbCommand.ExecuteReader();
                    int rows = 0;
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            rows++;
                            if (rows > 1)
                                throw new ConfigError("Multiple entries found for session in database");

                            cache_.CreationTime = DateTime.SpecifyKind((DateTime)reader[0], DateTimeKind.Utc);
                            //DateTime.SpecifyKind(cache_.CreationTime.Value, DateTimeKind.Utc);
                            cache_.NextTargetMsgSeqNum = (ulong)reader[1];
                            cache_.NextSenderMsgSeqNum = (ulong)reader[2];

                        }
                    }
                    else
                    {
                        DateTime createTime = cache_.CreationTime.HasValue ? cache_.CreationTime.Value : DateTime.UtcNow;
                        string insertQuery = "INSERT INTO " + sessions_table + " (beginstring, sendercompid, targetcompid, session_qualifier," +
                            "creation_time, incoming_seqnum, outgoing_seqnum) VALUES(" +
                            "'" + _sessionID.BeginString + "'," +
                            "'" + _sessionID.SenderCompID + "'," +
                            "'" + _sessionID.TargetCompID + "'," +
                            "'" + _sessionID.SessionQualifier + "'," +
                            "{ts '" + createTime.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'}," +
                            cache_.NextTargetMsgSeqNum + "," +
                            cache_.NextSenderMsgSeqNum + ")";

                        SqlCommand cmdInsert = sqlConnection.CreateCommand();
                        cmdInsert.CommandText = insertQuery;

                        if (0 == cmdInsert.ExecuteNonQuery())
                            throw new ConfigError("Unable to create session in database");
                    }
                }
            }
        }


        public ulong GetNextSenderMsgSeqNum()
        {
            return cache_.NextSenderMsgSeqNum;
        }

        public ulong GetNextTargetMsgSeqNum()
        {
            return cache_.NextTargetMsgSeqNum;
        }

        public void SetNextSenderMsgSeqNum(ulong value)
        {
            string queryString = string.Empty;

            queryString = queryString + "UPDATE " + sessions_table + " SET outgoing_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            try
            {
                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = queryString;

                        sqlConnection.Open();
                        var rowsAffected = dbCommand.ExecuteNonQuery();
                        cache_.NextSenderMsgSeqNum = value;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.Write("SetNextSenderMsgSeqNum: ");
                Console.WriteLine(ex.ToString());
            }

        }

        public void SetNextTargetMsgSeqNum(ulong value)
        {

            string queryString = string.Empty;


            queryString = queryString + "UPDATE " + sessions_table + " SET incoming_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";


            try
            {
                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = queryString;

                        sqlConnection.Open();
                        var rowsAffected = dbCommand.ExecuteNonQuery();
                        cache_.NextTargetMsgSeqNum = value;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.Write("SetNextTargetMsgSeqNum: ");
                Console.WriteLine(ex.ToString());
            }

        }

        public void IncrNextSenderMsgSeqNum()
        {
            cache_.IncrNextSenderMsgSeqNum();
            SetNextSenderMsgSeqNum(cache_.NextSenderMsgSeqNum);
        }

        public void IncrNextTargetMsgSeqNum()
        {
            cache_.IncrNextTargetMsgSeqNum();
            SetNextTargetMsgSeqNum(cache_.NextTargetMsgSeqNum);
        }

        public DateTime? CreationTime
        {
            get { return cache_.CreationTime; }
        }

        public ulong NextSenderMsgSeqNum { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public ulong NextTargetMsgSeqNum { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public DateTime GetCreationTime()
        {
            return cache_.CreationTime.Value;
        }

        public void Reset()
        {
            string queryString = string.Empty;

            queryString = queryString + "DELETE from " + messages_table + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            try
            {
                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = queryString;

                        sqlConnection.Open();
                        var rowsAffected = dbCommand.ExecuteNonQuery();

                    }
                }
            }
            catch (Exception ex)
            {
                Console.Write("Reset: ");
                Console.WriteLine(ex.ToString());
            }





            cache_.Reset();
            DateTime? time = cache_.CreationTime;

            string sqlTime = time.Value.ToString("yyyy-MM-dd HH:mm:ss.fff");

            queryString = "UPDATE " + sessions_table + " SET creation_time={ts '" + sqlTime + "'}, " +
                 "incoming_seqnum=" + cache_.NextTargetMsgSeqNum + ", "
                + "outgoing_seqnum=" + cache_.NextSenderMsgSeqNum + " WHERE "
                + "beginstring=" + "'" + _sessionID.BeginString + "' and "
                + "sendercompid=" + "'" + _sessionID.SenderCompID + "' and "
                + "targetcompid=" + "'" + _sessionID.TargetCompID + "' and "
                + "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            try
            {
                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = queryString;

                        sqlConnection.Open();
                        var rowsAffected = dbCommand.ExecuteNonQuery();

                    }
                }
            }
            catch (Exception ex)
            {
                Console.Write("Reset update session table: ");
                Console.WriteLine(ex.ToString());
            }

        }

        public void Refresh()
        {
            cache_.Reset();
            PopulateCache();
        }

        public void Dispose()
        {

        }

        public void Get(ulong startSeqNum, ulong endSeqNum, List<string> messages)
        {
            string queryString = string.Empty;


            queryString = queryString + "SELECT message FROM " + messages_table + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                "msgseqnum >=" + startSeqNum.ToString() + " and " + "msgseqnum<=" + endSeqNum.ToString() + " " +
                "ORDER BY msgseqnum";


            using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
            {
                using (var dbCommand = sqlConnection.CreateCommand())
                {
                    dbCommand.CommandText = queryString;

                    sqlConnection.Open();
                    var reader = dbCommand.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            messages.Add(reader[0].ToString());
                        }
                    }
                }
            }
        }

        public bool Set(ulong msgSeqNum, string msg)
        {
            string queryString = string.Empty;

            if (msg.Contains("'"))
                msg = msg.Replace("'", "''");

            queryString = "INSERT INTO " + messages_table +
                " (beginstring, sendercompid, targetcompid, session_qualifier, msgseqnum, message) " +
                "VALUES (" +

                "'" + _sessionID.BeginString + "'," +
                "'" + _sessionID.SenderCompID + "'," +
                "'" + _sessionID.TargetCompID + "'," +
                "'" + _sessionID.SessionQualifier + "'," +
                msgSeqNum.ToString() + "," +
                "'" + msg + "')";



            try
            {
                // Try to insert.  If it fails, try to UPDATE message:
                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = queryString;
                        sqlConnection.Open();

                        if (0 == dbCommand.ExecuteNonQuery())
                        {
                            string updateQuery = string.Empty;


                            updateQuery = "UDPATE " + messages_table + " SET message='" + msg + "' WHERE " +
                                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                                "msgseqnum=" + msgSeqNum.ToString();

                            SqlCommand cmdUpdate = sqlConnection.CreateCommand();
                            cmdUpdate.CommandText = updateQuery;
                            cmdUpdate.ExecuteNonQuery();
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Console.Write("Set: ");
                Console.WriteLine(ex.ToString());
            }

            return true;
        }
    }
}
