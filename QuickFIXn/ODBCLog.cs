﻿using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using QuickFix.Fields.Converters;

namespace QuickFix
{
    /// <summary>
    /// ODBC log implementation
    /// </summary>
    public class ODBCLog : ILog, System.IDisposable
    {
        public ODBCLog(SessionSettings settings, SessionID sessionID)
        {
            _sessionID = sessionID;
            _sessionSettings = settings;


            string user = string.Empty;
            if( _sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_USER))
                user = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_USER);
            string password = string.Empty;
            if(_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_PASSWORD))
                password = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_PASSWORD);

            string connectionString = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_CONNECTION_STRING);

            OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(connectionString);
            sb["UID"] = user;
            sb["PWD"] = password;
            odbc = new OdbcConnection(sb.ConnectionString);
            odbc.Open();
        }

        private string incomingTable = "messages_log";
        private string outgoingTable = "messages_log";
        private string eventTable = "event_log";
        private SessionID _sessionID;
        private SessionSettings _sessionSettings;
        private OdbcConnection odbc;

        public void Clear()
        {
            string whereClause = string.Empty;
            if(this._sessionID != null)
            {
                whereClause = whereClause + "WHERE BeginString = '" + _sessionID.BeginString + "' " +
                    "AND SenderCompID = '" + _sessionID.SenderCompID + "' " +
                    "AND TargetCompID = '" + _sessionID.TargetCompID + "' ";

                if (_sessionID.SessionQualifier.Length > 0)
                    whereClause = whereClause + "AND SessionQualifier = '" + _sessionID.SessionQualifier + "' ";
            }
            
            OdbcCommand cmd = new OdbcCommand("DELETE FROM " + eventTable + " " + whereClause, odbc);
            cmd.ExecuteNonQuery();

        }

        public void OnIncoming(string msg)
        {
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

            OdbcCommand cmd = new OdbcCommand(queryString, odbc);
            cmd.ExecuteNonQuery();
        }

        public void OnOutgoing(string msg)
        {
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

            OdbcCommand cmd = new OdbcCommand(queryString, odbc);
            cmd.ExecuteNonQuery();
        }

        public void OnEvent(string s)
        {
            string queryString = "INSERT INTO " + eventTable + " "  + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) " + "VALUES (" +
                "'" + ODBCHelper.DateTimeToODBCConverter(DateTime.UtcNow) + "', " +
                "'" + _sessionID.BeginString + "', " +
                "'" + _sessionID.SenderCompID + "', " +
                "'" + _sessionID.TargetCompID + "', ";

            if (_sessionID.SessionQualifier != null && _sessionID.SessionQualifier.Length > 0)
                queryString = queryString + "'" + _sessionID.SessionQualifier + "', ";
            else
                queryString = queryString + "'" + "NULL" + "', ";

            queryString = queryString + "'" +s + "')";

            OdbcCommand cmd = new OdbcCommand(queryString, odbc);
            cmd.ExecuteNonQuery();

            //throw new NotImplementedException();
        }

        public void Dispose()
        {
            //throw new NotImplementedException();
        }
    }
}
