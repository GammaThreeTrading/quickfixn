using Microsoft.Data.SqlClient;
using QuickFix.Logger;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QuickFix
{
    public class SQLLog : ILog, System.IDisposable
    {
        private string incomingTable = "messages_log";
        private string incomingBackupTable = "messages_backup_log";
        private string outgoingTable = "messages_log";
        private string outgoingBackupTable = "messages_backup_log";
        private string eventTable = "event_log";
        private string eventBackupTable = "event_backup_log";
        private SessionID _sessionID;
        private string _connectionString = string.Empty;
        private string _user = string.Empty;
        private string _pwd = string.Empty;
        private string _datasource = string.Empty;
        private string _initialcatalog = string.Empty;
        private SessionSettings _sessionSettings;


        public SQLLog(SessionSettings settings, SessionID sessionID)
        {
            _sessionID = sessionID;
            _sessionSettings = settings;

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_USER))
                _user = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_USER);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_PASSWORD))
                _pwd = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_PASSWORD);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_DATASOURCE))
                _datasource = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_DATASOURCE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_INITIAL_CATALOG))
                _initialcatalog = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_INITIAL_CATALOG);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_INCOMING_TABLE))
                incomingTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_INCOMING_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_INCOMING_BACKUP_TABLE))
                incomingBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_INCOMING_BACKUP_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_OUTGOING_TABLE))
                outgoingTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_OUTGOING_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_OUTGOING_BACKUP_TABLE))
                outgoingBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_OUTGOING_BACKUP_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_EVENT_TABLE))
                eventTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_EVENT_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_EVENT_BACKUP_TABLE))
                eventBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_EVENT_BACKUP_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_CONNECTION_STRING))
                _connectionString = _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_CONNECTION_STRING);



            //OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(_connectionString);
            //sb["UID"] = _user;
            //sb["PWD"] = _pwd;
            //OdbcConnection odbc = GetODBCConnection();

        }



        /// <summary>  
        /// You must edit the four 'my' string values.  
        /// </summary>  
        /// <returns>An ADO.NET connection string.</returns>  
        private string GetSqlConnectionString()
        {
            // Prepare the connection string to Azure SQL Database.  
            var sqlConnectionSB = new SqlConnectionStringBuilder();


            if(!string.IsNullOrEmpty(_connectionString))
            {
                sqlConnectionSB.ConnectionString = _connectionString;
            }
            else
            {
                // Change these values to your values.  
                sqlConnectionSB.DataSource = _datasource;//"tcp:myazuresqldbserver.database.windows.net,1433"; //["Server"]  
                sqlConnectionSB.InitialCatalog = _initialcatalog; //["Database"]  

                if(!string.IsNullOrEmpty(_user) && !string.IsNullOrEmpty(_pwd))
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

        //private OdbcConnection GetODBCConnection()
        //{
        //    OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(_connectionString);
        //    sb["UID"] = _user;
        //    sb["PWD"] = _pwd;
        //    OdbcConnection odbc = new OdbcConnection(sb.ConnectionString);
        //    odbc.Open();
        //    return odbc;
        //}


        public void Clear()
        {
            try
            {
                string whereClause = string.Empty;
                if (this._sessionID != null)
                {
                    whereClause = whereClause + "WHERE beginstring = '" + _sessionID.BeginString + "' " +
                        "AND sendercompid = '" + _sessionID.SenderCompID + "' " +
                        "AND targetcompid = '" + _sessionID.TargetCompID + "' ";

                    if (_sessionID.SessionQualifier.Length > 0)
                        whereClause = whereClause + "AND session_qualifier = '" + _sessionID.SessionQualifier + "' ";
                }


                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {
                    using (var dbCommand = sqlConnection.CreateCommand())
                    {
                        dbCommand.CommandText = "DELETE FROM " + eventTable + " " + whereClause;

                        sqlConnection.Open();
                        var rowsAffected = dbCommand.ExecuteNonQuery();

                    }
                }
            }
            catch (Exception ex)
            {
                Console.Write("OnClear: ");
                Console.WriteLine(ex.Message);
            }
        }

        public void Backup(DateTime? DateThreshold = null)
        {
            // Get a 10 Second window
            DateTime bufferTime = DateTime.UtcNow - new TimeSpan(0, 0, 10);
            if (DateThreshold != null)
            {
                bufferTime = DateThreshold.GetValueOrDefault(bufferTime);
            }
            //UtcTimeStamp time;
            string sqlTime = bufferTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

            string queryStringIncoming = string.Empty;
            string queryStringOutgoing = string.Empty;
            string queryStringEvent = string.Empty;
            string whereClause = string.Empty;

            queryStringIncoming = "INSERT INTO " + incomingBackupTable + " "
            + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) "
            + "select time, beginstring, sendercompid, targetcompid, session_qualifier, text ";

            queryStringOutgoing = "INSERT INTO " + outgoingBackupTable + " "
                + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) "
                + "select time, beginstring, sendercompid, targetcompid, session_qualifier, text ";

            queryStringEvent = "INSERT INTO " + eventBackupTable + " "
                + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) "
                + "select time, beginstring, sendercompid, targetcompid, session_qualifier, text ";


            // Event Where Clause
            string eventWhereClause = string.Empty;




            eventWhereClause = " WHERE "
                + "beginstring = '" + _sessionID.BeginString + "' "
                + "AND sendercompid = '" + _sessionID.SenderCompID + "' "
                + "AND targetcompid = '" + _sessionID.TargetCompID + "' "
                + "AND time < '" + sqlTime + "' ";

            if (!string.IsNullOrEmpty(_sessionID.SessionQualifier))
                eventWhereClause = eventWhereClause + "AND session_qualifier = '" + _sessionID.SessionQualifier + "'";


            //GammaThree - Backup messages into history, but ignore Heartbeats not associated with TestRequests:
            whereClause = " WHERE "
              + "beginstring = '" + _sessionID.BeginString + "' "
              + "AND sendercompid = '" + _sessionID.SenderCompID + "' "
              + "AND targetcompid = '" + _sessionID.TargetCompID + "' "
              + "AND time < '" + sqlTime + "' ";

            if (!string.IsNullOrEmpty(_sessionID.SessionQualifier))
                whereClause = whereClause + "AND session_qualifier = '" + _sessionID.SessionQualifier + "'";


            string incomingQuery = string.Empty;
            string incomingClearQuery = string.Empty;

            string outgoingQuery = string.Empty;
            string outgoingClearQuery = string.Empty;

            // Only query the non heartbeats.
            incomingQuery = queryStringIncoming + "FROM " + incomingTable + whereClause + "AND NOT( text like  '%' + char(01) + '35=0' + char(01) + '%')";
            incomingClearQuery = "delete " + incomingTable + " " + whereClause;

            outgoingQuery = queryStringOutgoing + "FROM " + outgoingTable + whereClause + "AND NOT( text like  '%' + char(01) + '35=0' + char(01) + '%')";
            outgoingClearQuery = "delete " + outgoingClearQuery + " " + whereClause;


            try
            {

                //string whereClause = string.Empty;
                if (this._sessionID != null)
                {
                    whereClause = whereClause + "WHERE beginstring = '" + _sessionID.BeginString + "' " +
                        "AND sendercompid = '" + _sessionID.SenderCompID + "' " +
                        "AND targetcompid = '" + _sessionID.TargetCompID + "' ";

                    if (_sessionID.SessionQualifier.Length > 0)
                        whereClause = whereClause + "AND session_qualifier = '" + _sessionID.SessionQualifier + "' ";
                }


                using (var sqlConnection = new SqlConnection(GetSqlConnectionString()))
                {

                    SqlCommand cmdIncoming = new SqlCommand(incomingQuery, sqlConnection);
                    SqlCommand cmdClearIncoming = new SqlCommand(incomingClearQuery, sqlConnection);
                    SqlCommand cmdOutgoing = new SqlCommand(outgoingQuery, sqlConnection);
                    SqlCommand cmdClearOutgoing = new SqlCommand(outgoingClearQuery, sqlConnection);

                    sqlConnection.Open();

                    cmdIncoming.ExecuteNonQuery();
                    cmdClearIncoming.ExecuteNonQuery();

                    // To prevent duplicates, check to see if incoming table is the same as outgoing.
                    if (incomingTable != outgoingTable)
                    {
                        cmdOutgoing.ExecuteNonQuery();
                        cmdClearOutgoing.ExecuteNonQuery();
                    }

                    // Backup to Event Log
                    string logQuery = queryStringEvent + " from " + eventTable + " " + eventWhereClause;
                    string logClearQuery = "delete " + eventTable + " " + eventWhereClause;
                    SqlCommand cmdEvent = new SqlCommand(logQuery, sqlConnection);
                    SqlCommand cmdClearEvent = new SqlCommand(logClearQuery, sqlConnection);
                    cmdEvent.ExecuteNonQuery();
                    cmdClearEvent.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Console.Write("OnBackup: ");
                Console.WriteLine(ex.Message);
            }


        }

        public void OnIncoming(string msg)
        {
            if (msg.Contains("'"))
                msg = msg.Replace("'", "''");

            string queryString = "INSERT INTO ";

            queryString = queryString + incomingTable + " " +
                "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) " +
                "VALUES (" +
                "{ts '" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'},";

            if (_sessionID != null)
            {
                queryString = queryString + "'" + _sessionID.BeginString + "'," +
                    "'" + _sessionID.SenderCompID + "'," +
                    "'" + _sessionID.TargetCompID + "',";
                if (_sessionID.SessionQualifier != null && _sessionID.SessionQualifier.Length > 0)
                    queryString = queryString + "'" + _sessionID.SessionQualifier + "',";
                else
                    queryString = queryString + "NULL,";
            }
            else
            {
                queryString = queryString + "NULL, NULL, NULL, NULL, ";
            }

            queryString = queryString + "'" + msg + "')";

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
                Console.Write("OnIncomingLog: ");
                Console.WriteLine(ex.Message);
            }
        }

        public void OnOutgoing(string msg)
        {
            if (msg.Contains("'"))
                msg = msg.Replace("'", "''");

            string queryString = "INSERT INTO ";

            queryString = queryString + outgoingTable + " " +
                "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) " +
                "VALUES (" +
                "{ts '" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + "'},";

            if (_sessionID != null)
            {
                queryString = queryString + "'" + _sessionID.BeginString + "'," +
                    "'" + _sessionID.SenderCompID + "'," +
                    "'" + _sessionID.TargetCompID + "',";
                if (_sessionID.SessionQualifier != null && _sessionID.SessionQualifier.Length > 0)
                    queryString = queryString + "'" + _sessionID.SessionQualifier + "',";
                else
                    queryString = queryString + "NULL,";
            }
            else
            {
                queryString = queryString + "NULL, NULL, NULL, NULL, ";
            }

            queryString = queryString + "'" + msg + "')";

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
                Console.Write("OnOutgoingLog: ");
                Console.WriteLine(ex.Message);
            }

        }

        public void OnEvent(string s)
        {
            try
            {
                if (s.Contains("'"))
                    s = s.Replace("'", "''");

                string queryString = "INSERT INTO " + eventTable + " " + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) " + "VALUES (" +
    "'" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + "', " +
    "'" + _sessionID.BeginString + "', " +
    "'" + _sessionID.SenderCompID + "', " +
    "'" + _sessionID.TargetCompID + "', ";

                if (_sessionID.SessionQualifier != null && _sessionID.SessionQualifier.Length > 0)
                    queryString
                        = queryString + "'" + _sessionID.SessionQualifier + "', ";
                else
                    queryString = queryString + "'" + "NULL" + "', ";

                queryString = queryString + "'" + s + "')";


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
                    Console.Write("OnEvent: ");
                    Console.WriteLine(ex.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


            //throw new NotImplementedException();
        }

        public void Dispose()
        {
            //throw new NotImplementedException();
        }
    }
}
