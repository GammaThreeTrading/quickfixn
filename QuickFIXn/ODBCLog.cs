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
        

        public ODBCLog(SessionSettings settings, SessionID sessionID)
        {
            _sessionID = sessionID;
            _sessionSettings = settings;

            if( _sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_USER))
                _user = _sessionSettings.Get(sessionID).GetString(SessionSettings.ODBC_LOG_USER);

            if(_sessionSettings.Get(sessionID).Has(SessionSettings.ODBC_LOG_PASSWORD))
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

            OdbcConnection odbc = GetODBCConnection();
        }


        private OdbcConnection GetODBCConnection()
        {
            OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(_connectionString);
            sb["UID"] = _user;
            sb["PWD"] = _pwd;
            OdbcConnection odbc = new OdbcConnection(sb.ConnectionString);
            odbc.Open();
            return odbc;
        }


        public void Clear()
        {
            OdbcConnection odbc = GetODBCConnection();

            string whereClause = string.Empty;
            if(this._sessionID != null)
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
            OdbcConnection odbc = GetODBCConnection();
            OdbcCommand cmd = new OdbcCommand(queryString, odbc);
            cmd.ExecuteNonQuery();
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
            OdbcConnection odbc = GetODBCConnection();
            OdbcCommand cmd = new OdbcCommand(queryString, odbc);
            cmd.ExecuteNonQuery();
        }

        public void OnEvent(string s)
        {
            try
            {
                if (s.Contains("'"))
                    s = s.Replace("'", "''");

                string queryString = "INSERT INTO " + eventTable + " " + "(time, beginstring, sendercompid, targetcompid, session_qualifier, text) " + "VALUES (" +
    "'" + ODBCHelper.DateTimeToODBCConverter(DateTime.UtcNow) + "', " +
    "'" + _sessionID.BeginString + "', " +
    "'" + _sessionID.SenderCompID + "', " +
    "'" + _sessionID.TargetCompID + "', ";

                if (_sessionID.SessionQualifier != null && _sessionID.SessionQualifier.Length > 0)
                    queryString
                        = queryString + "'" + _sessionID.SessionQualifier + "', ";
                else
                    queryString = queryString + "'" + "NULL" + "', ";

                queryString = queryString + "'" + s + "')";
                OdbcConnection odbc = GetODBCConnection();
                OdbcCommand cmd = new OdbcCommand(queryString, odbc);

                cmd.ExecuteNonQuery();
            }
            catch(Exception ex)
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
