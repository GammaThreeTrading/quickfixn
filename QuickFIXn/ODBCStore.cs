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
        private OdbcConnection odbc;

        public ODBCStore(SessionID sessionId, string user, string password, string connectionString)
        {
            _sessionID = sessionId;
            OdbcConnectionStringBuilder sb = new OdbcConnectionStringBuilder(connectionString);
            sb["UID"] = user;
            sb["PWD"] = password;
            odbc = new OdbcConnection(sb.ConnectionString);
            odbc.Open();
            PopulateCache();
        }

        public void PopulateCache()
        {
            string queryString = string.Empty;

            queryString = queryString + "SELECT creation_time, incoming_seqnum, outgoing_seqnum FROM sessions WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";


            using (OdbcCommand cmd = new OdbcCommand(queryString, odbc))
            {
                OdbcDataReader reader = cmd.ExecuteReader();
                int rows = 0;
                if(reader.HasRows)
                {
                    while(reader.Read())
                    {
                        rows++;
                        if(rows > 1)
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
                    string insertQuery = "INSERT INTO sessions (beginstring, sendercompid, targetcompid, session_qualifier," +
                        "creation_time, incoming_seqnum, outgoing_seqnum) VALUES(" +
                        "'" + _sessionID.BeginString + "'," +
                        "'" + _sessionID.SenderCompID + "'," +
                        "'" + _sessionID.TargetCompID + "'," +
                        "'" + _sessionID.SessionQualifier + "'," +
                        "{ts '" + ODBCHelper.DateTimeToODBCConverter(createTime) + "'}," +
                        cache_.GetNextTargetMsgSeqNum() + "," +
                        cache_.GetNextSenderMsgSeqNum() + ")";

                    OdbcCommand cmdInsert = new OdbcCommand(insertQuery, odbc);
                    if(0 == cmdInsert.ExecuteNonQuery())
                        throw new ConfigError( "Unable to create session in database" );
                }
                

            }

        }

        public void Get(int startSeqNum, int endSeqNum, List<string> messages)
        {
            string queryString = string.Empty;

            queryString = queryString + "SELECT message FROM messages WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                "msgseqnum >=" + startSeqNum.ToString() + " and " + "msgseqnum<=" + endSeqNum.ToString() + " " +
                "ORDER BY msgseqnum";

            using (OdbcCommand cmd = new OdbcCommand(queryString, odbc))
            {
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
            string msgCopy = msg;
            msgCopy.Replace("'", "''");

            queryString = queryString + "INSERT INTO messages " +
                "(beginstring, sendercompid, targetcompid, session_qualifier, msgseqnum, message) " +
                "VALUES (" +

                "'" + _sessionID.BeginString + "'," +
                "'" + _sessionID.SenderCompID + "'," +
                "'" + _sessionID.TargetCompID + "'," +
                "'" + _sessionID.SessionQualifier + "'," +
                msgSeqNum.ToString() + "," +
                "'" + msgCopy + "')";

            // Try to insert.  If it fails, try to UPDATE message:
            OdbcCommand cmdInsert = new OdbcCommand(queryString, odbc);
            if (0 == cmdInsert.ExecuteNonQuery())
            {
                string updateQuery = string.Empty;

                updateQuery = "UDPATE messages SET message='" + msgCopy + "' WHERE " +
                    "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                    "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                    "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                    "session_qualifier=" + "'" + _sessionID.SessionQualifier + "' and " +
                    "msgseqnum=" + msgSeqNum.ToString();

                OdbcCommand cmdUpdate = new OdbcCommand(updateQuery, odbc);
                cmdUpdate.ExecuteNonQuery();
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

            queryString = queryString + "UPDATE sessions SET outgoing_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
            cmdReset.ExecuteNonQuery();
            
            cache_.SetNextSenderMsgSeqNum(value);
        }

        public void SetNextTargetMsgSeqNum(int value)
        {

            string queryString = string.Empty;

            queryString = queryString + "UPDATE sessions SET incoming_seqnum=" + value.ToString() + " WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
            cmdReset.ExecuteNonQuery();

            cache_.SetNextTargetMsgSeqNum(value);
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
            cache_.Reset();
            string queryString = string.Empty;

            queryString = queryString + "DELETE from messages WHERE " +
                "beginstring=" + "'" + _sessionID.BeginString + "' and " +
                "sendercompid=" + "'" + _sessionID.SenderCompID + "' and " +
                "targetcompid=" + "'" + _sessionID.TargetCompID + "' and " +
                "session_qualifier=" + "'" + _sessionID.SessionQualifier + "'";

            OdbcCommand cmdReset = new OdbcCommand(queryString, odbc);
            cmdReset.ExecuteNonQuery();
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
