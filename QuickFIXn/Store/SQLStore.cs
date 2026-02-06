using Microsoft.Data.SqlClient;
using QuickFix.Store;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace QuickFix
{
    /// <summary>
    /// Robust, high-throughput SQLStore implementation:
    /// - Uses "using" blocks (pooled connections) - no shared SqlConnection
    /// - Parameterized SQL (safe + plan reuse)
    /// - Safe quoting of table identifiers from config (prevents injection via table names)
    /// - Optional per-instance lock for safety if store methods are invoked concurrently
    /// - Upsert for message storage
    /// </summary>
    public class SQLStore : IMessageStore, IDisposable
    {
        private readonly MemoryStore cache_ = new MemoryStore();

        private readonly SessionID _sessionID;
        private readonly SessionSettings _sessionSettings;

        private string messages_table = "messages";
        private string sessions_table = "sessions";

        private string _connectionString = string.Empty;
        private string _user = string.Empty;
        private string _pwd = string.Empty;
        private string _datasource = string.Empty;
        private string _initialcatalog = string.Empty;

        private bool _ignoreAdminMessages = true;

        // Quote-safe identifiers (computed once)
        private readonly string _messagesTableQ;
        private readonly string _sessionsTableQ;

        // Session key fields (constant)
        private readonly string _begin;
        private readonly string _sender;
        private readonly string _target;
        private readonly string _qual; // may be empty string in your data model

        // Safety: If QuickFIXn ever calls store concurrently, this avoids cache / seqnum races.
        private readonly object _storeLock = new();

        private static readonly Regex SafeIdentifier = new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

        public SQLStore(SessionID sessionId, string user, string password, string connectionString, SessionSettings settings)
        {
            _sessionID = sessionId ?? throw new ArgumentNullException(nameof(sessionId));
            _sessionSettings = settings ?? throw new ArgumentNullException(nameof(settings));

            _begin = _sessionID.BeginString;
            _sender = _sessionID.SenderCompID;
            _target = _sessionID.TargetCompID;
            _qual = _sessionID.SessionQualifier ?? string.Empty;

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_SESSION_TABLE))
                sessions_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_SESSION_TABLE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_MESSAGES_TABLE))
                messages_table = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_MESSAGES_TABLE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_DATASOURCE))
                _datasource = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_DATASOURCE);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_INITIAL_CATALOG))
                _initialcatalog = _sessionSettings.Get(_sessionID).GetString(SessionSettings.SQL_STORE_INITIAL_CATALOG);

            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_IGNORE_ADMIN_MESSAGES))
                _ignoreAdminMessages = _sessionSettings.Get(_sessionID).GetBool(SessionSettings.SQL_STORE_IGNORE_ADMIN_MESSAGES);

            _connectionString = connectionString ?? string.Empty;
            _user = user ?? string.Empty;
            _pwd = password ?? string.Empty;

            _sessionsTableQ = QuoteName(sessions_table);
            _messagesTableQ = QuoteName(messages_table);

            PopulateCache();
        }

        public void Dispose()
        {
            // Nothing to dispose because we use pooled connections per call
        }

        // ---------------------------
        // Connection string builder
        // ---------------------------

        private string GetSqlConnectionString()
        {
            var sb = new SqlConnectionStringBuilder();

            if (!string.IsNullOrEmpty(_connectionString))
            {
                sb.ConnectionString = _connectionString;
            }
            else
            {
                sb.DataSource = _datasource;
                sb.InitialCatalog = _initialcatalog;

                if (!string.IsNullOrEmpty(_user) && !string.IsNullOrEmpty(_pwd))
                {
                    sb.UserID = _user;
                    sb.Password = _pwd;
                    sb.IntegratedSecurity = false;
                }
                else
                {
                    sb.IntegratedSecurity = true;
                }
            }

            // Only set defaults if not already provided
            if (!sb.ContainsKey("Encrypt")) sb.Encrypt = true;
            if (!sb.ContainsKey("TrustServerCertificate")) sb.TrustServerCertificate = false;
            if (!sb.ContainsKey("Connect Timeout")) sb.ConnectTimeout = 15;
            if (!sb.ContainsKey("ConnectRetryCount")) sb.ConnectRetryCount = 3;
            if (!sb.ContainsKey("ConnectRetryInterval")) sb.ConnectRetryInterval = 2;

            if (!sb.ContainsKey("Application Name")) sb.ApplicationName = "QuickFIXn-SQLStore";

            return sb.ToString();
        }

        // ---------------------------
        // Cache/session bootstrap
        // ---------------------------

        public void PopulateCache()
        {
            lock (_storeLock)
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                conn.Open();

                // Load session row (creation_time, incoming_seqnum, outgoing_seqnum)
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $@"
SELECT creation_time, incoming_seqnum, outgoing_seqnum
FROM {_sessionsTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                    AddSessionKeyParams(cmd);

                    using var reader = cmd.ExecuteReader();
                    int rows = 0;

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            rows++;
                            if (rows > 1)
                                throw new ConfigError("Multiple entries found for session in database");

                            cache_.CreationTime = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);

                            // These are typically bigint in QuickFIX stores; your existing code converts loosely.
                            cache_.NextTargetMsgSeqNum = Convert.ToUInt64(reader.GetValue(1)); // incoming_seqnum
                            cache_.NextSenderMsgSeqNum = Convert.ToUInt64(reader.GetValue(2)); // outgoing_seqnum
                        }
                        return;
                    }
                }

                // If not found: create it
                var createTime = cache_.CreationTime.HasValue ? cache_.CreationTime.Value : DateTime.UtcNow;

                using (var cmdInsert = conn.CreateCommand())
                {
                    cmdInsert.CommandText = $@"
INSERT INTO {_sessionsTableQ}
(beginstring, sendercompid, targetcompid, session_qualifier, creation_time, incoming_seqnum, outgoing_seqnum)
VALUES
(@begin, @sender, @target, @qual, @creation_time, @incoming_seqnum, @outgoing_seqnum);";

                    AddSessionKeyParams(cmdInsert);
                    cmdInsert.Parameters.Add("@creation_time", SqlDbType.DateTime2).Value = createTime;
                    cmdInsert.Parameters.Add("@incoming_seqnum", SqlDbType.BigInt).Value = (long)cache_.NextTargetMsgSeqNum;
                    cmdInsert.Parameters.Add("@outgoing_seqnum", SqlDbType.BigInt).Value = (long)cache_.NextSenderMsgSeqNum;

                    var rows = cmdInsert.ExecuteNonQuery();
                    if (rows == 0)
                        throw new ConfigError("Unable to create session in database");
                }
            }
        }

        public void Refresh()
        {
            lock (_storeLock)
            {
                cache_.Reset();
                PopulateCache();
            }
        }

        // ---------------------------
        // IMessageStore: seqnums
        // ---------------------------

        public ulong GetNextSenderMsgSeqNum() => cache_.NextSenderMsgSeqNum;
        public ulong GetNextTargetMsgSeqNum() => cache_.NextTargetMsgSeqNum;

        public void SetNextSenderMsgSeqNum(ulong value)
        {
            lock (_storeLock)
            {
                try
                {
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    conn.Open();

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"
UPDATE {_sessionsTableQ}
SET outgoing_seqnum = @value
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                    cmd.Parameters.Add("@value", SqlDbType.BigInt).Value = (long)value;
                    AddSessionKeyParams(cmd);

                    cmd.ExecuteNonQuery();
                    cache_.NextSenderMsgSeqNum = value;
                }
                catch (Exception ex)
                {
                    Console.Write("SetNextSenderMsgSeqNum: ");
                    Console.WriteLine(ex);
                }
            }
        }

        public void SetNextTargetMsgSeqNum(ulong value)
        {
            lock (_storeLock)
            {
                try
                {
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    conn.Open();

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"
UPDATE {_sessionsTableQ}
SET incoming_seqnum = @value
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                    cmd.Parameters.Add("@value", SqlDbType.BigInt).Value = (long)value;
                    AddSessionKeyParams(cmd);

                    cmd.ExecuteNonQuery();
                    cache_.NextTargetMsgSeqNum = value;
                }
                catch (Exception ex)
                {
                    Console.Write("SetNextTargetMsgSeqNum: ");
                    Console.WriteLine(ex);
                }
            }
        }

        public void IncrNextSenderMsgSeqNum()
        {
            lock (_storeLock)
            {
                cache_.IncrNextSenderMsgSeqNum();
                SetNextSenderMsgSeqNum(cache_.NextSenderMsgSeqNum);
            }
        }

        public void IncrNextTargetMsgSeqNum()
        {
            lock (_storeLock)
            {
                cache_.IncrNextTargetMsgSeqNum();
                SetNextTargetMsgSeqNum(cache_.NextTargetMsgSeqNum);
            }
        }

        public DateTime? CreationTime => cache_.CreationTime;

        public ulong NextSenderMsgSeqNum
        {
            get => cache_.NextSenderMsgSeqNum;
            set
            {
                lock (_storeLock)
                {
                    cache_.NextSenderMsgSeqNum = value;
                    SetNextSenderMsgSeqNum(value);
                }
            }
        }

        public ulong NextTargetMsgSeqNum
        {
            get => cache_.NextTargetMsgSeqNum;
            set
            {
                lock (_storeLock)
                {
                    cache_.NextTargetMsgSeqNum = value;
                    SetNextTargetMsgSeqNum(value);
                }
            }
        }

        public DateTime GetCreationTime() => cache_.CreationTime!.Value;

        // ---------------------------
        // IMessageStore: messages
        // ---------------------------

        public void Get(ulong startSeqNum, ulong endSeqNum, List<string> messages)
        {
            if (messages == null) throw new ArgumentNullException(nameof(messages));

            lock (_storeLock)
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $@"
SELECT message
FROM {_messagesTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual
  AND msgseqnum >= @start
  AND msgseqnum <= @end
ORDER BY msgseqnum;";

                AddSessionKeyParams(cmd);
                cmd.Parameters.Add("@start", SqlDbType.BigInt).Value = (long)startSeqNum;
                cmd.Parameters.Add("@end", SqlDbType.BigInt).Value = (long)endSeqNum;

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    messages.Add(reader.GetString(0));
                }
            }
        }

        public bool Set(ulong msgSeqNum, string msg)
        {
            if (_ignoreAdminMessages)
            {
                try
                {
                    if (Message.IsAdminMsgType(Message.GetMsgType(msg)))
                        return true;
                }
                catch
                {
                    // Ignore parse exceptions
                }
            }

            lock (_storeLock)
            {
                try
                {
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    conn.Open();

                    // Fast upsert: update first, insert if not found (single round-trip)
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"
UPDATE {_messagesTableQ}
SET message = @message
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual
  AND msgseqnum = @seq;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO {_messagesTableQ}
    (beginstring, sendercompid, targetcompid, session_qualifier, msgseqnum, message)
    VALUES
    (@begin, @sender, @target, @qual, @seq, @message);
END";

                    AddSessionKeyParams(cmd);
                    cmd.Parameters.Add("@seq", SqlDbType.BigInt).Value = (long)msgSeqNum;
                    cmd.Parameters.Add("@message", SqlDbType.NVarChar, -1).Value = msg ?? string.Empty;

                    cmd.ExecuteNonQuery();
                    return true;
                }
                catch (Exception ex)
                {
                    Console.Write("Set: ");
                    Console.WriteLine(ex);
                    return false;
                }
            }
        }

        public void Reset()
        {
            lock (_storeLock)
            {
                try
                {
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    conn.Open();

                    // Delete messages for this session
                    using (var cmdDel = conn.CreateCommand())
                    {
                        cmdDel.CommandText = $@"
DELETE FROM {_messagesTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                        AddSessionKeyParams(cmdDel);
                        cmdDel.ExecuteNonQuery();
                    }

                    cache_.Reset();
                    var time = cache_.CreationTime ?? DateTime.UtcNow;

                    // Update session row
                    using (var cmdUpd = conn.CreateCommand())
                    {
                        cmdUpd.CommandText = $@"
UPDATE {_sessionsTableQ}
SET creation_time = @creation_time,
    incoming_seqnum = @incoming,
    outgoing_seqnum = @outgoing
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                        AddSessionKeyParams(cmdUpd);
                        cmdUpd.Parameters.Add("@creation_time", SqlDbType.DateTime2).Value = time;
                        cmdUpd.Parameters.Add("@incoming", SqlDbType.BigInt).Value = (long)cache_.NextTargetMsgSeqNum;
                        cmdUpd.Parameters.Add("@outgoing", SqlDbType.BigInt).Value = (long)cache_.NextSenderMsgSeqNum;

                        cmdUpd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    Console.Write("Reset: ");
                    Console.WriteLine(ex);
                }
            }
        }

        // ---------------------------
        // Helpers
        // ---------------------------

        private void AddSessionKeyParams(SqlCommand cmd)
        {
            cmd.Parameters.Add("@begin", SqlDbType.NVarChar, 32).Value = _begin;
            cmd.Parameters.Add("@sender", SqlDbType.NVarChar, 64).Value = _sender;
            cmd.Parameters.Add("@target", SqlDbType.NVarChar, 64).Value = _target;

            // Your schema uses varchar(64) for session_qualifier and allows NULL.
            // Your existing code stores it as '' sometimes. We'll preserve your behavior by using empty string.
            cmd.Parameters.Add("@qual", SqlDbType.NVarChar, 64).Value = _qual;
        }

        private static string QuoteName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

            // Allow either "table" or "schema.table"
            var parts = tableName.Split('.');
            if (parts.Length == 1)
            {
                var t = parts[0].Trim();
                if (!SafeIdentifier.IsMatch(t))
                    throw new ArgumentException($"Unsafe table identifier: {tableName}");
                return $"[dbo].[{t}]";
            }
            if (parts.Length == 2)
            {
                var schema = parts[0].Trim();
                var t = parts[1].Trim();
                if (!SafeIdentifier.IsMatch(schema) || !SafeIdentifier.IsMatch(t))
                    throw new ArgumentException($"Unsafe table identifier: {tableName}");
                return $"[{schema}].[{t}]";
            }

            throw new ArgumentException($"Unsupported table identifier format: {tableName}");
        }
    }
}
