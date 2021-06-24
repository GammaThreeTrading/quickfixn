using System;
using System.Data.Odbc;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;

namespace QuickFix
{


    /// <summary>
    /// ODBCFildHybrid log implementation
    /// </summary>
    public class ODBCFileHybridLog : ILog, System.IDisposable
    {
        private BlockingCollection<string> incomingMessageItems = new BlockingCollection<string>();
        private BlockingCollection<string> outgoingMessageItems = new BlockingCollection<string>();
        private CancellationTokenSource cts = new CancellationTokenSource();
        private object sync_ = new object();
        private Thread _incomingLogThread = null;
        private Thread _outgoingLogThread = null;

        private System.IO.StreamWriter messageLog_;
        private System.IO.StreamWriter eventLog_;

        private string messageLogFileName_;
        private string eventLogFileName_;


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
        private SessionSettings _sessionSettings;


        public ODBCFileHybridLog(SessionSettings settings, SessionID sessionID)
        {
            _sessionID = sessionID;
            _sessionSettings = settings;

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_USER))
                _user = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_USER);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_PASSWORD))
                _pwd = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_PASSWORD);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_INCOMING_TABLE))
                incomingTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_INCOMING_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_INCOMING_BACKUP_TABLE))
                incomingBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_INCOMING_BACKUP_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_OUTGOING_TABLE))
                outgoingTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_OUTGOING_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_OUTGOING_BACKUP_TABLE))
                outgoingBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_OUTGOING_BACKUP_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_EVENT_TABLE))
                eventTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_EVENT_TABLE);

            if (_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_EVENT_BACKUP_TABLE))
                eventBackupTable = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_EVENT_BACKUP_TABLE);

            _connectionString = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_CONNECTION_STRING);


            string fileLogPath = string.Empty;
            if (_sessionSettings.Get(sessionID).Has(SessionSettings.HYBRID_FILE_LOG_PATH))
                fileLogPath = _sessionSettings.Get(sessionID).GetString(SessionSettings.HYBRID_FILE_LOG_PATH);


            Init(fileLogPath, Prefix(sessionID));

            _incomingLogThread = new Thread(new ThreadStart(IncomingLogWorker));
            _incomingLogThread.Start();
            _outgoingLogThread = new Thread(new ThreadStart(OutgoingLogWorker));
            _outgoingLogThread.Start();

        }

        private void IncomingLogWorker()
        {

            string nextItem = string.Empty;
            // IsCompleted == (IsAddingCompleted && Count == 0)
            while (!cts.IsCancellationRequested)
            {

                try
                {
                    if (!incomingMessageItems.TryTake(out nextItem, -1, cts.Token))
                    {
                        Console.WriteLine(" incoming cancellation token set");
                    }
                    else
                    {
                        OnIncoming(nextItem);
                    }
                }

                catch (OperationCanceledException)
                {
                    Console.WriteLine("outgoing cancellation token set");
                    break;
                }

            }


        }

        private void OutgoingLogWorker()
        {

            string nextItem = string.Empty;
            // IsCompleted == (IsAddingCompleted && Count == 0)
            while (!cts.IsCancellationRequested)
            {

                try
                {
                    if (!outgoingMessageItems.TryTake(out nextItem, -1, cts.Token))
                    {
                        Console.WriteLine(" Take Blocked");
                    }
                    else
                    {
                        OnOutgoing(nextItem);
                    }
                }

                catch (OperationCanceledException)
                {
                    Console.WriteLine("Taking canceled.");
                    break;
                }

            }
        }

        public static string Prefix(SessionID sessionID)
        {
            System.Text.StringBuilder prefix = new System.Text.StringBuilder(sessionID.BeginString)
                .Append('-').Append(sessionID.SenderCompID);
            if (SessionID.IsSet(sessionID.SenderSubID))
                prefix.Append('_').Append(sessionID.SenderSubID);
            if (SessionID.IsSet(sessionID.SenderLocationID))
                prefix.Append('_').Append(sessionID.SenderLocationID);
            prefix.Append('-').Append(sessionID.TargetCompID);
            if (SessionID.IsSet(sessionID.TargetSubID))
                prefix.Append('_').Append(sessionID.TargetSubID);
            if (SessionID.IsSet(sessionID.TargetLocationID))
                prefix.Append('_').Append(sessionID.TargetLocationID);

            if (SessionID.IsSet(sessionID.SessionQualifier))
                prefix.Append('-').Append(sessionID.SessionQualifier);

            return prefix.ToString();
        }

        private void Init(string fileLogPath, string prefix)
        {
            if (!System.IO.Directory.Exists(fileLogPath))
                System.IO.Directory.CreateDirectory(fileLogPath);

            messageLogFileName_ = System.IO.Path.Combine(fileLogPath, prefix + ".messages.current.log");
            eventLogFileName_ = System.IO.Path.Combine(fileLogPath, prefix + ".event.current.log");

            messageLog_ = new System.IO.StreamWriter(messageLogFileName_, true);
            eventLog_ = new System.IO.StreamWriter(eventLogFileName_, true);

            messageLog_.AutoFlush = true;
            eventLog_.AutoFlush = true;
        }

        private string GetOdbcConnectionString()
        {
            OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(_connectionString);
            sb["UID"] = _user;
            sb["PWD"] = _pwd;
            return sb.ConnectionString;
        }


        public void Clear()
        {
            using (OdbcConnection odbc = new OdbcConnection(GetOdbcConnectionString()))
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

                OdbcCommand cmd = new OdbcCommand("DELETE FROM " + eventTable + " " + whereClause, odbc);
                cmd.ExecuteNonQuery();
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
            string sqlTime = ODBCHelper.DateTimeToODBCConverter(bufferTime);

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

            using (OdbcConnection odbc = new OdbcConnection(GetOdbcConnectionString()))
            {
                odbc.Open();

                OdbcCommand cmdIncoming = new OdbcCommand(incomingQuery, odbc);
                OdbcCommand cmdClearIncoming = new OdbcCommand(incomingClearQuery, odbc);
                OdbcCommand cmdOutgoing = new OdbcCommand(outgoingQuery, odbc);
                OdbcCommand cmdClearOutgoing = new OdbcCommand(outgoingClearQuery, odbc);
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
                OdbcCommand cmdEvent = new OdbcCommand(logQuery, odbc);
                OdbcCommand cmdClearEvent = new OdbcCommand(logClearQuery, odbc);
                cmdEvent.ExecuteNonQuery();
                cmdClearEvent.ExecuteNonQuery();

                odbc.Close();
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
                "{ts '" + ODBCHelper.DateTimeToODBCConverter(DateTime.UtcNow) + "'},";

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

            using (OdbcConnection odbc = new OdbcConnection(GetOdbcConnectionString()))
            {
                odbc.Open();
                OdbcCommand cmd = new OdbcCommand(queryString, odbc);
                cmd.ExecuteNonQuery();
                odbc.Close();
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
                "{ts '" + ODBCHelper.DateTimeToODBCConverter(DateTime.UtcNow) + "'},";

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

            using (OdbcConnection odbc = new OdbcConnection(GetOdbcConnectionString()))
            {
                odbc.Open();
                OdbcCommand cmd = new OdbcCommand(queryString, odbc);
                cmd.ExecuteNonQuery();
                odbc.Close();
            }

        }

        public void OnEvent(string s)
        {
            DisposedCheck();

            lock (sync_)
            {
                eventLog_.WriteLine(Fields.Converters.DateTimeConverter.Convert(System.DateTime.UtcNow) + " : " + s);
            }
        }






        private void DisposedCheck()
        {
            if (_disposed)
                throw new System.ObjectDisposedException(this.GetType().Name);
        }
        #region IDisposable Members
        public void Dispose()
        {
            cts.Cancel();
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool _disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                if (messageLog_ != null) { messageLog_.Dispose(); }
                if (eventLog_ != null) { eventLog_.Dispose(); }

                messageLog_ = null;
                eventLog_ = null;
            }
            _disposed = true;
        }
        ~ODBCFileHybridLog() => Dispose(false);
        #endregion
    }
}
