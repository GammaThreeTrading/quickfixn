using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using QuickFix.Fields.Converters;

namespace QuickFix
{
    static public class ODBCHelper
    {
        public static string DateTimeToODBCConverter(DateTime dt)
        {
            string strFinal = string.Empty;
            //string strDate = DateTimeConverter.ConvertDateOnly(dt);
            string strDate = dt.ToString("yyyy-MM-dd");
            string strTime = DateTimeConverter.ConvertTimeOnly(dt, true);

            strFinal = strDate + " " + strTime;
            return strFinal;
        }
    }
    public class ODBCStore : IMessageStore
    {
        private MemoryStore cache_ = new MemoryStore();

        private SessionID _sessionID;
        private SessionSettings _sessionSettings;

        private string messages_table = "messages";
        private string sessions_table = "sessions";
        private string _connectionString = string.Empty;
        private string _user = string.Empty;
        private string _pwd = string.Empty;

        public ODBCStore(SessionID sessionId, string user, string password, string connectionString, SessionSettings settings)
        {
            _sessionID = sessionId;
            _sessionSettings = settings;

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.ODBC_STORE_SESSION_TABLE))
                sessions_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.ODBC_STORE_SESSION_TABLE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.ODBC_STORE_MESSAGES_TABLE))
                messages_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.ODBC_STORE_MESSAGES_TABLE);

            _connectionString = connectionString;
            _user = user;
            _pwd = password;

            PopulateCache();
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
        public void PopulateCache()
        {
            string queryString = string.Empty;

            queryString = "SELECT creation_time, incoming_seqnum, outgoing_seqnum FROM " + sessions_table + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            using (OdbcConnection odbc = GetODBCConnection())
            {
                OdbcCommand cmd = new OdbcCommand(queryString, odbc);
                OdbcDataReader reader = cmd.ExecuteReader();
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
                        cache_.SetNextTargetMsgSeqNum((int)reader[1]);
                        cache_.SetNextSenderMsgSeqNum((int)reader[2]);

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
                        "{ts '" + ODBCHelper.DateTimeToODBCConverter(createTime) + "'}," +
                        cache_.GetNextTargetMsgSeqNum() + "," +
                        cache_.GetNextSenderMsgSeqNum() + ")";

                    OdbcCommand cmdInsert = new OdbcCommand(insertQuery, odbc);
                    if (0 == cmdInsert.ExecuteNonQuery())
                        throw new ConfigError("Unable to create session in database");
                }
            }

        }

        public void Get(int startSeqNum, int endSeqNum, List<string> messages)
        {
            string queryString = string.Empty;


            queryString = queryString + "SELECT message FROM " + messages_table + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                "msgseqnum >=" + startSeqNum.ToString() + " and " + "msgseqnum<=" + endSeqNum.ToString() + " " +
                "ORDER BY msgseqnum";


            using (OdbcConnection odbc = GetODBCConnection())
            {
                OdbcCommand cmd = new OdbcCommand(queryString, odbc);
                OdbcDataReader reader = cmd.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        messages.Add(reader[0].ToString());
                    }
                }
            }
        }

        public bool Set(int msgSeqNum, string msg)
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
                using (OdbcConnection odbc = GetODBCConnection())
                {
                    OdbcCommand cmdInsert = new OdbcCommand(queryString, odbc);
                    if (0 == cmdInsert.ExecuteNonQuery())
                    {
                        string updateQuery = string.Empty;


                        updateQuery = "UDPATE " + messages_table + " SET message='" + msg + "' WHERE " +
                            "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                            "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                            "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                            "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                            "msgseqnum=" + msgSeqNum.ToString();

                        OdbcCommand cmdUpdate = new OdbcCommand(updateQuery, odbc);
                        cmdUpdate.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }




            return true;
        }

        public int GetNextSenderMsgSeqNum()
        {
            return cache_.GetNextSenderMsgSeqNum();
        }

        public int GetNextTargetMsgSeqNum()
        {
            return cache_.GetNextTargetMsgSeqNum();
        }

        public void SetNextSenderMsgSeqNum(int value)
        {
            string queryString = string.Empty;

            queryString = queryString + "UPDATE " + sessions_table + " SET outgoing_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            try
            {
                using (OdbcConnection odbc = GetODBCConnection())
                {
                    OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
                    cmdReset.ExecuteNonQuery();
                    cache_.SetNextSenderMsgSeqNum(value);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

        }

        public void SetNextTargetMsgSeqNum(int value)
        {

            string queryString = string.Empty;


            queryString = queryString + "UPDATE " + sessions_table + " SET incoming_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";


            try
            {
                using (OdbcConnection odbc = GetODBCConnection())
                {
                    OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
                    cmdReset.ExecuteNonQuery();

                    cache_.SetNextTargetMsgSeqNum(value);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

        }

        public void IncrNextSenderMsgSeqNum()
        {
            cache_.IncrNextSenderMsgSeqNum();
            SetNextSenderMsgSeqNum(cache_.GetNextSenderMsgSeqNum());
        }

        public void IncrNextTargetMsgSeqNum()
        {
            cache_.IncrNextTargetMsgSeqNum();
            SetNextTargetMsgSeqNum(cache_.GetNextTargetMsgSeqNum());
        }

        public DateTime? CreationTime
        {
            get { return cache_.CreationTime; }
        }

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
                using (OdbcConnection odbc = GetODBCConnection())
                {
                    OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
                    cmdReset.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }





            cache_.Reset();
            DateTime? time = cache_.CreationTime;

            string sqlTime = ODBCHelper.DateTimeToODBCConverter(time.Value);

            queryString = "UPDATE " + sessions_table + " SET creation_time={ts '" + sqlTime + "'}, " +
                 "incoming_seqnum=" + cache_.GetNextTargetMsgSeqNum() + ", "
                + "outgoing_seqnum=" + cache_.GetNextSenderMsgSeqNum() + " WHERE "
                + "beginstring=" + "'" + _sessionID.BeginString + "' and "
                + "sendercompid=" + "'" + _sessionID.SenderCompID + "' and "
                + "targetcompid=" + "'" + _sessionID.TargetCompID + "' and "
                + "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            try
            {
                using (OdbcConnection odbc = GetODBCConnection())
                {
                    OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
                    cmdReset.ExecuteNonQuery();
                }

            }
            catch (Exception ex)
            {
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
    }
}
