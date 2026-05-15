using Microsoft.Data.SqlClient;
using QuickFix.Logger;
using QuickFix.Store;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace QuickFix
{
    /// <summary>
    /// SQLStore with full write-behind for the session hot path.
    ///
    /// All SQL writes (message persist, seqnum updates, Reset) are enqueued to a
    /// single background writer via an ordered channel. The session thread only
    /// touches the in-memory cache, so no logon, send, or receive ever blocks on
    /// SqlConnection.Open() or DELETE/UPDATE latency.
    ///
    /// Correctness: cache_ is authoritative for the session thread. SQL eventually
    /// catches up in enqueue order. On hard crash mid-flush the sessions table can
    /// be behind the cache; on restart, PopulateCache reads the stale state and
    /// the IsNewSession path repairs it for new-day startups. For mid-day crashes
    /// the same seqnum-mismatch exposure applies as in QuickFIX/J's async stores.
    /// </summary>
    public class SQLStore : IMessageStore, IDisposable
    {
        // -----------------------------------------------------------------------
        // Channel op types
        // -----------------------------------------------------------------------
        private abstract record QueueOp;
        private sealed record WriteMsgOp(ulong SeqNum, string Message) : QueueOp;
        private sealed record ResetOp(DateTime CreationTime, ulong NextTargetSeq, ulong NextSenderSeq, DateTime EnqueuedAt) : QueueOp;
        private sealed record SetSeqOp(bool IsSender, ulong Value) : QueueOp;
        // FlushBarrierOp lets Get() wait until all prior ops have been applied
        // to SQL before reading. Without it there's a small window where Reset()
        // has updated the cache and enqueued the DELETE but not yet executed it,
        // and a concurrent Get() would read stale rows.
        private sealed record FlushBarrierOp(TaskCompletionSource<bool> Tcs) : QueueOp;

        private const int BatchSize = 50;
        // Default writer wake-up debounce. Configurable per session via the
        // SQLStoreFlushIntervalMs setting; see _flushIntervalMs below.
        private const int DefaultFlushIntervalMs = 100;
        // Sanity bounds on the configured value. Below ~10ms the debounce
        // produces almost no batching benefit; above ~2s the at-risk window
        // on hard crash grows uncomfortably even for simulator use.
        private const int MinFlushIntervalMs = 10;
        private const int MaxFlushIntervalMs = 2000;
        // After an unexpected writer crash, back off briefly before restarting so
        // we don't busy-loop if the crash is deterministic.
        private const int WriterRestartDelayMs = 2000;
        // Upper bound on how long Get() will wait for in-flight ops to drain
        // before reading SQL. A healthy flush completes well under 1s even with
        // a Reset in the queue, so 5s gives ~10× headroom for transient slowness
        // while still failing fast enough that the SocketReader thread doesn't
        // miss heartbeats during a SQL storm. On timeout we proceed with the
        // SELECT; the FIX protocol's GapFill recovery handles any stale-read
        // edge case downstream.
        private const int GetFlushTimeoutMs = 5_000;

        private readonly Channel<QueueOp> _channel = Channel.CreateUnbounded<QueueOp>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        private readonly Task _writerTask;
        private readonly CancellationTokenSource _cts = new();

        // -----------------------------------------------------------------------
        // Session state / cache
        // -----------------------------------------------------------------------
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

        // Writer wake-up debounce, in milliseconds. Controls the trade-off
        // between batch size (efficiency) and at-risk window on crash. Read
        // from session settings at construction; clamped to a sane range.
        private readonly int _flushIntervalMs;

        private readonly string _messagesTableQ;
        private readonly string _sessionsTableQ;

        private readonly string _begin;
        private readonly string _sender;
        private readonly string _target;
        private readonly string _qual;

        // _storeLock serializes cache mutation + channel writes from the session
        // thread. The background writer never takes it.
        private readonly object _storeLock = new();

        private static readonly Regex SafeIdentifier = new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

        // The session's ILog. Wired by Session after construction so the store can
        // surface diagnostic events (e.g., Reset timings) through whatever log the
        // session is configured with — ScreenLog, FileLog, SQLLog, or Composite.
        // Null until Session wires it; in that case diagnostics fall back to Console.
        public ILog? Log { get; set; }

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

            int configuredFlushMs = DefaultFlushIntervalMs;
            if (_sessionSettings.Get(_sessionID).Has(SessionSettings.SQL_STORE_FLUSH_INTERVAL_MS))
                configuredFlushMs = (int)_sessionSettings.Get(_sessionID).GetLong(SessionSettings.SQL_STORE_FLUSH_INTERVAL_MS);
            _flushIntervalMs = Math.Clamp(configuredFlushMs, MinFlushIntervalMs, MaxFlushIntervalMs);

            _connectionString = connectionString ?? string.Empty;
            _user = user ?? string.Empty;
            _pwd = password ?? string.Empty;

            _sessionsTableQ = QuoteName(sessions_table);
            _messagesTableQ = QuoteName(messages_table);

            // PopulateCache is sync. It only SELECTs (and at most INSERTs once) on the
            // sessions table — no DELETEs, no contention. Fast even on cold pool.
            PopulateCache();

            // Eager writer start. Reset() and the SetNext* paths enqueue ops, so the
            // writer must be running from the start (no more lazy-on-first-Set).
            _writerTask = Task.Run(() => BackgroundWriterAsync(_cts.Token));
        }

        public void Dispose()
        {
            // Signal end-of-stream. The writer's outer loop sees the channel
            // complete, finishes the explicit final-drain block, and returns.
            _channel.Writer.Complete();

            var sw = Stopwatch.StartNew();
            bool drained;
            Exception? failure = null;
            try
            {
                drained = _writerTask?.Wait(TimeSpan.FromSeconds(5)) ?? true;
            }
            catch (Exception ex)
            {
                drained = false;
                failure = ex;
            }
            sw.Stop();

            // Best-effort: estimate how much was left when we gave up. Reader
            // is single-threaded so the count is stable once the wait returns.
            // (If drained=true the channel is fully consumed; the count is 0.)
            int pendingAtComplete = 0;
            if (!drained && _channel.Reader.TryPeek(out _))
            {
                // TryPeek confirmed >=1; count by draining a snapshot view.
                while (_channel.Reader.TryRead(out _)) pendingAtComplete++;
            }

            LogDisposeOutcome(sw.ElapsedMilliseconds, drained, pendingAtComplete, failure);

            _cts.Dispose();
        }

        // -----------------------------------------------------------------------
        // Dispose-timing diagnostic. Same routing as LogResetTiming: through the
        // session's ILog (which is still alive — SessionState disposes the store
        // before the log) with a Console fallback. Critical for incident triage
        // after a forced shutdown: did the drain finish, or did SCM cut us off?
        // -----------------------------------------------------------------------
        private void LogDisposeOutcome(long elapsedMs, bool drained, int pendingAtComplete, Exception? failure)
        {
            string status;
            if (failure is not null)
                status = $"FAIL ({failure.GetType().Name}: {failure.Message})";
            else if (drained)
                status = "OK";
            else
                status = "TIMEOUT";

            var text =
                $"SQLStore Dispose: drained_in={elapsedMs}ms " +
                $"pending_at_complete={pendingAtComplete} status={status}";

            Console.WriteLine($"{DateTime.UtcNow:O} [{_sender}->{_target}] {text}");

            try { Log?.OnEvent(text); }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLStore [{_sender}->{_target}]: ILog.OnEvent for dispose outcome failed: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // Connection string
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
        // PopulateCache / Refresh — sync, called only at startup or explicit
        // Session.Refresh().
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

                var createTime = cache_.CreationTime ?? DateTime.UtcNow;

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
            // NOTE: With write-behind, in-flight ops in the channel haven't reached SQL
            // yet. Refresh re-reads SQL and can therefore roll back in-memory state to
            // a slightly-stale snapshot. In practice Refresh is invoked at logon time
            // (RefreshOnLogon) when traffic is quiet, so the race window is small —
            // but be aware.
            lock (_storeLock)
            {
                cache_.Reset();
                PopulateCache();
            }
        }

        // -----------------------------------------------------------------------
        // Seqnums — cache updates synchronously, SQL persistence enqueued.
        // -----------------------------------------------------------------------
        public ulong GetNextSenderMsgSeqNum() => cache_.NextSenderMsgSeqNum;
        public ulong GetNextTargetMsgSeqNum() => cache_.NextTargetMsgSeqNum;

        public void SetNextSenderMsgSeqNum(ulong value)
        {
            lock (_storeLock)
            {
                cache_.NextSenderMsgSeqNum = value;
                _channel.Writer.TryWrite(new SetSeqOp(IsSender: true, value));
            }
        }

        public void SetNextTargetMsgSeqNum(ulong value)
        {
            lock (_storeLock)
            {
                cache_.NextTargetMsgSeqNum = value;
                _channel.Writer.TryWrite(new SetSeqOp(IsSender: false, value));
            }
        }

        public void IncrNextSenderMsgSeqNum()
        {
            lock (_storeLock)
            {
                cache_.IncrNextSenderMsgSeqNum();
                _channel.Writer.TryWrite(new SetSeqOp(IsSender: true, cache_.NextSenderMsgSeqNum));
            }
        }

        public void IncrNextTargetMsgSeqNum()
        {
            lock (_storeLock)
            {
                cache_.IncrNextTargetMsgSeqNum();
                _channel.Writer.TryWrite(new SetSeqOp(IsSender: false, cache_.NextTargetMsgSeqNum));
            }
        }

        public DateTime? CreationTime => cache_.CreationTime;

        public ulong NextSenderMsgSeqNum
        {
            get => cache_.NextSenderMsgSeqNum;
            set => SetNextSenderMsgSeqNum(value);
        }

        public ulong NextTargetMsgSeqNum
        {
            get => cache_.NextTargetMsgSeqNum;
            set => SetNextTargetMsgSeqNum(value);
        }

        public DateTime GetCreationTime() => cache_.CreationTime!.Value;

        // -----------------------------------------------------------------------
        // Get — sync. Used by resend handling, not on the logon hot path.
        //
        // To avoid reading SQL state that is older than what the cache says
        // (e.g., Reset has run on the cache but the DELETE hasn't flushed yet),
        // we enqueue a FlushBarrierOp and wait for the writer to reach it. The
        // wait is bounded by GetFlushTimeoutMs; on timeout we proceed with the
        // read rather than block the session, accepting the small risk of stale
        // data — which the FIX gap-fill protocol can recover from anyway.
        // -----------------------------------------------------------------------
        public void Get(ulong startSeqNum, ulong endSeqNum, List<string> messages)
        {
            if (messages == null) throw new ArgumentNullException(nameof(messages));

            WaitForFlush(GetFlushTimeoutMs);

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

        // -----------------------------------------------------------------------
        // Insert a barrier into the channel and wait for the writer to reach it.
        // Used by Get() so reads observe all prior writes/resets/seqnum updates.
        // Bounded wait — never blocks indefinitely even if the writer is wedged.
        // -----------------------------------------------------------------------
        private void WaitForFlush(int timeoutMs)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_channel.Writer.TryWrite(new FlushBarrierOp(tcs)))
            {
                // Channel is closed (shutting down). Best we can do is proceed.
                return;
            }

            try { tcs.Task.Wait(timeoutMs); }
            catch { /* tolerate cancellation/aggregation exceptions; the read can proceed either way */ }
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
                catch { }
            }

            _channel.Writer.TryWrite(new WriteMsgOp(msgSeqNum, msg));
            return true;
        }

        // -----------------------------------------------------------------------
        // Reset — synchronous on the cache, asynchronous on SQL.
        //
        // The session thread (which called us via _state.Reset) gets correct
        // cache state immediately, so the very next GenerateLogon sees seqnum 1.
        // The actual DELETE messages + UPDATE sessions happens later on the
        // background writer, where 12-second stalls are harmless.
        //
        // Channel ordering preserves correctness: pending pre-reset writes are
        // dropped here (they'd be DELETEd anyway), and post-reset writes are
        // enqueued AFTER the ResetOp, so they reach SQL after the DELETE+UPDATE.
        // -----------------------------------------------------------------------
        public void Reset()
        {
            lock (_storeLock)
            {
                // Drop pending pre-reset writes. They are about to be DELETEd
                // anyway, and dropping them avoids briefly INSERTing rows the
                // upcoming DELETE would erase a moment later.
                while (_channel.Reader.TryRead(out _)) { }

                cache_.Reset();
                var creationTime = cache_.CreationTime ?? DateTime.UtcNow;
                var nextTarget = cache_.NextTargetMsgSeqNum;
                var nextSender = cache_.NextSenderMsgSeqNum;

                _channel.Writer.TryWrite(new ResetOp(creationTime, nextTarget, nextSender, DateTime.UtcNow));
            }
        }

        // -----------------------------------------------------------------------
        // Background writer
        //
        // Single-reader. Processes the channel in order. Consecutive WriteMsgOps
        // are batched into one MERGE; ResetOp, SetSeqOp, and FlushBarrierOp act
        // as barriers that flush any pending writes first, then run their own SQL
        // (or, for FlushBarrierOp, just signal completion).
        //
        // Self-healing: the outer while restarts the inner processing loop after
        // a brief delay if an unexpected exception escapes the per-op try/catch.
        // Without this, a bug here would silently kill the writer task, the
        // channel would fill unboundedly, and the session would have no clue.
        // -----------------------------------------------------------------------
        private async Task BackgroundWriterAsync(CancellationToken ct)
        {
            var writeBatch = new List<WriteMsgOp>(BatchSize);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                    {
                        // Brief delay so message bursts coalesce into a single MERGE.
                        try { await Task.Delay(_flushIntervalMs, ct).ConfigureAwait(false); }
                        catch (OperationCanceledException) { break; }

                        while (_channel.Reader.TryRead(out var op))
                            await DispatchOpAsync(op, writeBatch).ConfigureAwait(false);

                        if (writeBatch.Count > 0)
                        {
                            await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                            writeBatch.Clear();
                        }
                    }

                    // Channel completed (Dispose called) — drain whatever's left and exit cleanly.
                    while (_channel.Reader.TryRead(out var op))
                        await DispatchOpAsync(op, writeBatch).ConfigureAwait(false);
                    if (writeBatch.Count > 0)
                        await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException)
                {
                    return; // shutting down
                }
                catch (Exception ex)
                {
                    // Unknown crash. Per-op handlers should have caught SQL errors;
                    // anything reaching here is unexpected (NRE, OOM, etc.). Don't
                    // trust writeBatch state.
                    LogWriterError(
                        $"BackgroundWriter crashed, restarting in {WriterRestartDelayMs}ms: " +
                        $"[{ex.GetType().Name}] {ex.Message}");
                    writeBatch.Clear();

                    try { await Task.Delay(WriterRestartDelayMs, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                }
            }
        }

        // -----------------------------------------------------------------------
        // Dispatch one op. Non-write ops act as flush barriers: pending writes
        // are flushed before the op runs so SQL state is consistent at the moment
        // the op (or its FlushBarrier signal) takes effect.
        // -----------------------------------------------------------------------
        private async Task DispatchOpAsync(QueueOp op, List<WriteMsgOp> writeBatch)
        {
            switch (op)
            {
                case WriteMsgOp w:
                    writeBatch.Add(w);
                    if (writeBatch.Count >= BatchSize)
                    {
                        await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                        writeBatch.Clear();
                    }
                    break;

                case ResetOp r:
                    if (writeBatch.Count > 0)
                    {
                        await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                        writeBatch.Clear();
                    }
                    await ApplyResetAsync(r).ConfigureAwait(false);
                    break;

                case SetSeqOp s:
                    if (writeBatch.Count > 0)
                    {
                        await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                        writeBatch.Clear();
                    }
                    await ApplySetSeqAsync(s).ConfigureAwait(false);
                    break;

                case FlushBarrierOp b:
                    if (writeBatch.Count > 0)
                    {
                        await FlushWritesAsync(writeBatch).ConfigureAwait(false);
                        writeBatch.Clear();
                    }
                    b.Tcs.TrySetResult(true);
                    break;
            }
        }

        // -----------------------------------------------------------------------
        // Writer-level error logging. Goes to Console + ILog (when wired). Used
        // for restart notifications — these are rare and important enough that
        // we want them surfaced loudly.
        // -----------------------------------------------------------------------
        private void LogWriterError(string message)
        {
            var line = $"{DateTime.UtcNow:O} SQLStore [{_sender}->{_target}]: {message}";
            Console.WriteLine(line);
            try { Log?.OnEvent($"SQLStore writer: {message}"); }
            catch { /* don't let logging take down the writer we just restarted */ }
        }

        // -----------------------------------------------------------------------
        // ApplyResetAsync — timed so we can root-cause the morning stall
        // without instrumenting the session thread.
        //
        // Captures: queue latency (enqueue→start), open ms, delete ms, update ms,
        // rows deleted, and final status. Surfaced via the session's ILog so the
        // line lands wherever the session normally logs (event_log for SQLLog,
        // file for FileLog, screen for ScreenLog, etc.).
        // -----------------------------------------------------------------------
        private async Task ApplyResetAsync(ResetOp op)
        {
            var queueLatencyMs = (long)(DateTime.UtcNow - op.EnqueuedAt).TotalMilliseconds;
            var step = new Stopwatch();
            long openMs = -1, deleteMs = -1, updateMs = -1;
            int deletedRows = -1;
            Exception? failure = null;

            try
            {
                await ExecuteWithDeadlockRetryAsync(async () =>
                {
                    // Reset counters for each retry attempt so the log reflects the
                    // attempt that actually succeeded (or last failed).
                    openMs = -1; deleteMs = -1; updateMs = -1; deletedRows = -1;

                    step.Restart();
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    await conn.OpenAsync().ConfigureAwait(false);
                    openMs = step.ElapsedMilliseconds;

                    step.Restart();
                    using (var cmdDel = conn.CreateCommand())
                    {
                        cmdDel.CommandText = $@"
DELETE FROM {_messagesTableQ} WITH (ROWLOCK)
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";
                        AddSessionKeyParams(cmdDel);
                        deletedRows = await cmdDel.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    deleteMs = step.ElapsedMilliseconds;

                    step.Restart();
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
                        cmdUpd.Parameters.Add("@creation_time", SqlDbType.DateTime2).Value = op.CreationTime;
                        cmdUpd.Parameters.Add("@incoming", SqlDbType.BigInt).Value = (long)op.NextTargetSeq;
                        cmdUpd.Parameters.Add("@outgoing", SqlDbType.BigInt).Value = (long)op.NextSenderSeq;
                        await cmdUpd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    updateMs = step.ElapsedMilliseconds;
                }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            LogResetTiming(queueLatencyMs, openMs, deleteMs, updateMs, deletedRows, failure);
        }

        // -----------------------------------------------------------------------
        // Reset-timing diagnostic. Routes through the session's ILog so the
        // message goes wherever the session is configured to log: ScreenLog,
        // FileLog, SQLLog, CompositeLog — whichever the user set up.
        //
        // Runs on the background writer task, so any synchronous cost of the
        // log call never lands on the session thread.
        // -----------------------------------------------------------------------
        private void LogResetTiming(
            long queueLatencyMs, long openMs, long deleteMs, long updateMs,
            int deletedRows, Exception? failure)
        {
            var status = failure is null
                ? "OK"
                : $"FAIL ({failure.GetType().Name}: {failure.Message})";

            var text =
                $"SQLStore Reset timing: queue_latency={queueLatencyMs}ms " +
                $"open={openMs}ms delete={deleteMs}ms update={updateMs}ms " +
                $"deleted_rows={deletedRows} status={status}";

            // Echo to Console too — covers the case where Session hasn't wired up
            // Log yet (e.g., a startup-time Reset) or the configured log dropped
            // the event.
            Console.WriteLine($"{DateTime.UtcNow:O} [{_sender}->{_target}] {text}");

            try { Log?.OnEvent(text); }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLStore [{_sender}->{_target}]: ILog.OnEvent for reset timing failed: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // ApplySetSeqAsync — UPDATE one seqnum column in the sessions row.
        // -----------------------------------------------------------------------
        private async Task ApplySetSeqAsync(SetSeqOp op)
        {
            var column = op.IsSender ? "outgoing_seqnum" : "incoming_seqnum";
            try
            {
                await ExecuteWithDeadlockRetryAsync(async () =>
                {
                    using var conn = new SqlConnection(GetSqlConnectionString());
                    await conn.OpenAsync().ConfigureAwait(false);

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $@"
UPDATE {_sessionsTableQ} WITH (ROWLOCK)
SET {column} = @value
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND session_qualifier = @qual;";
                    cmd.Parameters.Add("@value", SqlDbType.BigInt).Value = (long)op.Value;
                    AddSessionKeyParams(cmd);
                    await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLStore [{_sender}->{_target}]: SetSeq ({column}) failed: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // FlushWritesAsync — single-statement MERGE for a batch of WriteMsgOps.
        //
        // No more _dbWriteLock / _flushCts: the channel is the serializer now.
        // -----------------------------------------------------------------------
        private async Task FlushWritesAsync(List<WriteMsgOp> batch)
        {
            try
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                await conn.OpenAsync().ConfigureAwait(false);

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

                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLStore [{_sender}->{_target}]: FlushWrites error: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // Async deadlock-retry helper. Error 1205 is "deadlock victim"; retrying
        // after a brief random jitter resolves it in almost all cases.
        // -----------------------------------------------------------------------
        private static async Task ExecuteWithDeadlockRetryAsync(Func<Task> action, int maxRetries = 3)
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    await action().ConfigureAwait(false);
                    return;
                }
                catch (SqlException ex) when (ex.Number == 1205 && attempt < maxRetries)
                {
                    int delayMs = 20 * (1 << attempt) + Random.Shared.Next(10);
                    await Task.Delay(delayMs).ConfigureAwait(false);
                }
            }
        }

        // -----------------------------------------------------------------------
        // Helpers
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
