using Microsoft.Data.SqlClient;
using QuickFix.Store;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace QuickFix
{
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

        private readonly string _messagesTableQ;
        private readonly string _sessionsTableQ;

        private readonly string _begin;
        private readonly string _sender;
        private readonly string _target;
        private readonly string _qual;

        private readonly object _storeLock = new();

        // -----------------------------------------------------------------------
        // _dbWriteLock: serializes FlushBatchAsync vs Reset() at the .NET level,
        // preventing concurrent INSERT/DELETE on the messages table (SQL deadlocks).
        // Per-instance; no cross-session effects.
        // -----------------------------------------------------------------------
        private readonly SemaphoreSlim _dbWriteLock = new SemaphoreSlim(1, 1);

        // -----------------------------------------------------------------------
        // _flushCts: allows Reset() to cancel an in-progress FlushBatchAsync so
        // _dbWriteLock.Wait() in Reset() returns quickly (not blocking session locks).
        // Replaced under _flushCtsLock after each Reset().
        // -----------------------------------------------------------------------
        private CancellationTokenSource _flushCts = new CancellationTokenSource();
        private readonly object _flushCtsLock = new();

        private static readonly Regex SafeIdentifier = new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

        // -----------------------------------------------------------------------
        // Async write-behind: ONLY for Set().
        // Everything else (Reset, PopulateCache, etc.) is original and untouched.
        // -----------------------------------------------------------------------

        private record WriteItem(ulong SeqNum, string Message);

        private const int BatchSize = 50;
        private const int FlushIntervalMs = 100;

        private readonly Channel<WriteItem> _channel =
            Channel.CreateUnbounded<WriteItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        private Task? _writerTask;
        private readonly CancellationTokenSource _cts = new();
        private int _writerStarted = 0; // Interlocked: 0=not started, 1=started


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

            // Writer starts lazily on first real message (see Set()).
            // This guarantees it is NOT running during the very first startup Reset().
            PopulateCache();
        }

        public void Dispose()
        {
            _channel.Writer.Complete();
            _writerTask?.Wait(TimeSpan.FromSeconds(5));
            _flushCts.Dispose();
            _cts.Dispose();
        }

        // -----------------------------------------------------------------------
        // Connection string (original — unchanged)
        // -----------------------------------------------------------------------

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

            if (!sb.ContainsKey("Encrypt")) sb.Encrypt = true;
            if (!sb.ContainsKey("TrustServerCertificate")) sb.TrustServerCertificate = false;
            if (!sb.ContainsKey("Connect Timeout")) sb.ConnectTimeout = 15;
            if (!sb.ContainsKey("ConnectRetryCount")) sb.ConnectRetryCount = 3;
            if (!sb.ContainsKey("ConnectRetryInterval")) sb.ConnectRetryInterval = 2;
            if (!sb.ContainsKey("Application Name")) sb.ApplicationName = "QuickFIXn-SQLStore";

            return sb.ToString();
        }

        // -----------------------------------------------------------------------
        // Cache/session bootstrap (original — unchanged)
        // -----------------------------------------------------------------------

        public void PopulateCache()
        {
            lock (_storeLock)
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                conn.Open();

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
                            cache_.NextTargetMsgSeqNum = Convert.ToUInt64(reader.GetValue(1));
                            cache_.NextSenderMsgSeqNum = Convert.ToUInt64(reader.GetValue(2));
                        }
                        return;
                    }
                }

                var createTime = cache_.CreationTime.HasValue ? cache_.CreationTime.Value : DateTime.UtcNow;

                using (var cmdInsert = conn.CreateCommand())
                {
                    cmdInsert.CommandText = $@"
INSERT INTO {_sessionsTableQ} WITH (ROWLOCK)
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

        // -----------------------------------------------------------------------
        // IMessageStore: seqnums (original — unchanged)
        // -----------------------------------------------------------------------

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
UPDATE {_sessionsTableQ} WITH (ROWLOCK)
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
UPDATE {_sessionsTableQ} WITH (ROWLOCK)
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

        // -----------------------------------------------------------------------
        // IMessageStore: messages
        // -----------------------------------------------------------------------

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
                    messages.Add(reader.GetString(0));
            }
        }

        // CHANGED: was synchronous DB call. Now enqueues to background writer.
        public bool Set(ulong msgSeqNum, string msg)
        {
            if (_ignoreAdminMessages)
            {
                try
                {
                    if (Message.IsAdminMsgType(Message.GetMsgType(msg)))
                        return true;
                }
                catch { }
            }

            // Start writer on first real message.
            // By the time the first app message arrives, logon and Reset() are done.
            if (Interlocked.CompareExchange(ref _writerStarted, 1, 0) == 0)
                _writerTask = Task.Run(() => BackgroundWriterAsync(_cts.Token));

            _channel.Writer.TryWrite(new WriteItem(msgSeqNum, msg));
            return true;
        }

        // -----------------------------------------------------------------------
        // Retry helper — SQL Server error 1205 is a deadlock victim; retrying
        // after a brief random jitter resolves it in almost all cases.
        // Only used for Reset() which runs on the session thread under locks.
        // -----------------------------------------------------------------------

        private static void ExecuteWithDeadlockRetry(Action action, int maxRetries = 3)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    action();
                    return;
                }
                catch (SqlException ex) when (ex.Number == 1205 && attempt < maxRetries)
                {
                    // 1205 = deadlock victim — back off and retry
                    int delayMs = 20 * (1 << attempt) + Random.Shared.Next(10); // 20, 40, 80ms + jitter
                    Thread.Sleep(delayMs);
                }
            }
        }

        // -----------------------------------------------------------------------
        // Reset
        //
        // KEY CHANGE: Cancel any in-progress FlushBatchAsync before waiting for
        // _dbWriteLock. This prevents Reset() from blocking under Session._sync
        // while the background writer works through a 50-item batch.
        //
        // The cancellation is safe: Reset() drains the channel first, so any
        // rows mid-flush are rows we're about to DELETE. Cancelling the INSERT
        // and then DELETEing the table is a no-op at worst.
        //
        // After Reset(), a fresh CancellationTokenSource is installed so the
        // background writer can resume flushing normally on the next logon.
        // -----------------------------------------------------------------------

        public void Reset()
        {
            // 1. Drain queued writes — Reset() is about to delete them anyway.
            while (_channel.Reader.TryRead(out _)) { }

            // 2. Cancel any in-progress flush AND install a pre-cancelled token so that
            //    any background writer thread that already drained the channel into its
            //    local batch variable cannot re-insert those rows after the DELETE below.
            //
            //    Race prevented:
            //      - Writer drains channel into local batch BEFORE Reset() drains it
            //      - Reset() drains channel (nothing left), cancels oldCts
            //      - Writer picks up _flushCts — must see a cancelled token, not a fresh one
            //      - Fresh token installed only AFTER _dbWriteLock is released, so writer
            //        cannot flush stale pre-Reset() rows into a post-Reset() clean table.
            CancellationTokenSource oldCts;
            lock (_flushCtsLock)
            {
                oldCts = _flushCts;
                var preCancel = new CancellationTokenSource();
                preCancel.Cancel(); // pre-cancelled: blocks any flush attempt during Reset()
                _flushCts = preCancel;
            }
            oldCts.Cancel();
            oldCts.Dispose();

            // 3. Wait for the (now-cancelled) flush to release the lock.
            //    This should return in microseconds.
            _dbWriteLock.Wait();
            try
            {
                lock (_storeLock)
                {
                    ExecuteWithDeadlockRetry(() =>
                    {
                        using var conn = new SqlConnection(GetSqlConnectionString());
                        conn.Open();

                        using (var cmdDel = conn.CreateCommand())
                        {
                            cmdDel.CommandText = $@"
DELETE FROM {_messagesTableQ} WITH (ROWLOCK)
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";

                            AddSessionKeyParams(cmdDel);
                            cmdDel.ExecuteNonQuery();
                        }

                        cache_.Reset();
                        var time = cache_.CreationTime ?? DateTime.UtcNow;

                        using (var cmdUpd = conn.CreateCommand())
                        {
                            cmdUpd.CommandText = $@"
UPDATE {_sessionsTableQ} WITH (ROWLOCK)
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
                    });
                }
            }
            finally
            {
                // Install a fresh CTS only NOW — after the DELETE is committed and the lock
                // is about to be released. The background writer cannot acquire _dbWriteLock
                // until after this point, so it will always see a valid uncancelled token
                // and will only flush messages enqueued AFTER this Reset() completes.
                lock (_flushCtsLock)
                {
                    _flushCts.Dispose(); // dispose the pre-cancelled one
                    _flushCts = new CancellationTokenSource();
                }
                _dbWriteLock.Release();
            }
        }

        // -----------------------------------------------------------------------
        // Background writer — messages table only
        // -----------------------------------------------------------------------

        private async Task BackgroundWriterAsync(CancellationToken ct)
        {
            var batch = new List<WriteItem>(BatchSize);
            try
            {
                while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                {
                    await Task.Delay(FlushIntervalMs, ct).ConfigureAwait(false);

                    while (_channel.Reader.TryRead(out var item) && batch.Count < BatchSize)
                        batch.Add(item);

                    if (batch.Count > 0)
                    {
                        CancellationToken flushToken;
                        lock (_flushCtsLock) { flushToken = _flushCts.Token; }

                        await FlushBatchAsync(batch, flushToken).ConfigureAwait(false);
                        batch.Clear();
                    }
                }
            }
            catch (OperationCanceledException) { }

            // Drain on shutdown
            while (_channel.Reader.TryRead(out var item))
                batch.Add(item);
            if (batch.Count > 0)
            {
                CancellationToken flushToken;
                lock (_flushCtsLock) { flushToken = _flushCts.Token; }
                await FlushBatchAsync(batch, flushToken).ConfigureAwait(false);
            }
        }

        // -----------------------------------------------------------------------
        // FlushBatchAsync — true single-statement batch MERGE
        //
        // KEY CHANGE: was N sequential UPDATE+INSERT round-trips (up to 50×~10ms).
        // Now a single MERGE statement with a VALUES table constructor.
        // One round-trip regardless of batch size: worst-case latency ~10ms not ~500ms.
        //
        // Also takes a CancellationToken so Reset() can abort it quickly.
        // -----------------------------------------------------------------------

        private async Task FlushBatchAsync(List<WriteItem> batch, CancellationToken ct)
        {
            // If ct is already cancelled before we acquire, WaitAsync throws without
            // taking the lock. The catch returns immediately, so the finally below
            // is only ever reached when the lock was successfully acquired.
            try
            {
                await _dbWriteLock.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Reset() cancelled before we even acquired — nothing to release.
                return;
            }

            try
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                await conn.OpenAsync(ct).ConfigureAwait(false);

                // Build a single MERGE using a VALUES constructor.
                // e.g. USING (VALUES (@seq0,@msg0),(@seq1,@msg1),...) AS src(seq,msg)
                var sql = new StringBuilder();
                sql.Append($@"
MERGE {_messagesTableQ} WITH (ROWLOCK) AS tgt
USING (VALUES ");

                for (int i = 0; i < batch.Count; i++)
                {
                    if (i > 0) sql.Append(',');
                    sql.Append($"(@seq{i},@msg{i})");
                }

                sql.Append(@") AS src(msgseqnum, message)
ON  tgt.beginstring       = @begin
AND tgt.sendercompid      = @sender
AND tgt.targetcompid      = @target
AND tgt.session_qualifier = @qual
AND tgt.msgseqnum         = src.msgseqnum
WHEN MATCHED THEN
    UPDATE SET tgt.message = src.message
WHEN NOT MATCHED THEN
    INSERT (beginstring, sendercompid, targetcompid, session_qualifier, msgseqnum, message)
    VALUES (@begin, @sender, @target, @qual, src.msgseqnum, src.message);");

                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql.ToString();
                AddSessionKeyParams(cmd);

                for (int i = 0; i < batch.Count; i++)
                {
                    cmd.Parameters.Add($"@seq{i}", SqlDbType.BigInt).Value = (long)batch[i].SeqNum;
                    cmd.Parameters.Add($"@msg{i}", SqlDbType.NVarChar, -1).Value = batch[i].Message ?? string.Empty;
                }

                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Reset() cancelled us — expected, not an error. Rows will be deleted by Reset().
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLStore [{_sender}->{_target}]: FlushBatch error: {ex.Message}");
            }
            finally
            {
                _dbWriteLock.Release();
            }
        }

        // -----------------------------------------------------------------------
        // Helpers (original — unchanged)
        // -----------------------------------------------------------------------

        private void AddSessionKeyParams(SqlCommand cmd)
        {
            cmd.Parameters.Add("@begin", SqlDbType.NVarChar, 32).Value = _begin;
            cmd.Parameters.Add("@sender", SqlDbType.NVarChar, 64).Value = _sender;
            cmd.Parameters.Add("@target", SqlDbType.NVarChar, 64).Value = _target;
            cmd.Parameters.Add("@qual", SqlDbType.NVarChar, 64).Value = _qual;
        }

        private static string QuoteName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

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
