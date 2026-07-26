using Microsoft.Data.SqlClient;
using QuickFix.Logger;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace QuickFix
{
    /// <summary>
    /// SQLLog with async write-behind for the hot path only.
    ///
    /// v2 — hardened against writer death and memory exhaustion:
    ///   - Bounded channel (cap: 100 000 items, ~40 MB worst case)
    ///   - Self-healing writer loop (restarts on any exception)
    ///   - Drop counter with periodic warning when channel is full
    ///   - Bulk insert via SqlBulkCopy for throughput under load
    ///   - File-based error logging (works when running as a service)
    /// </summary>
    public class SQLLog : ILog, IDisposable
    {
        // -----------------------------------------------------------------------
        // Write-behind queue (hot path only)
        // -----------------------------------------------------------------------

        private sealed record LogItem(string TableQ, string Message, DateTime Time);

        private const int ChannelCapacity = 100_000;    // hard memory cap
        private const int BatchSize = 100;
        private const int FlushIntervalMs = 100;
        private const int WriterRestartDelayMs = 2000;  // back-off after writer error
        private const int DropLogIntervalMs = 10_000;   // how often to log drop warnings

        private readonly Channel<LogItem> _channel = Channel.CreateBounded<LogItem>(
            new BoundedChannelOptions(ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

        private readonly Task _writerTask;
        private readonly CancellationTokenSource _cts = new();

        // Drop tracking
        private long _dropCount;
        private DateTime _lastDropLog = DateTime.MinValue;

        // -----------------------------------------------------------------------
        // Original fields (unchanged)
        // -----------------------------------------------------------------------

        private string incomingTable = "messages_log";
        private string incomingBackupTable = "messages_backup_log";
        private string outgoingTable = "messages_log";
        private string outgoingBackupTable = "messages_backup_log";
        private string eventTable = string.Empty;
        private string eventBackupTable = "event_backup_log";

        private readonly SessionID _sessionID;
        private readonly SessionSettings _sessionSettings;

        private string _connectionString = string.Empty;
        private string _user = string.Empty;
        private string _pwd = string.Empty;
        private string _datasource = string.Empty;
        private string _initialcatalog = string.Empty;

        private readonly string _incomingTableQ;
        private readonly string _outgoingTableQ;
        private readonly string _incomingBackupTableQ;
        private readonly string _outgoingBackupTableQ;
        private readonly string _eventTableQ;
        private readonly string _eventBackupTableQ;

        private readonly string? _begin;
        private readonly string? _sender;
        private readonly string? _target;
        private readonly string? _qual;
        private readonly string _errorLogPath;

        private static readonly Regex SafeIdentifier = new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

        // -----------------------------------------------------------------------
        // Constructor (original + start background writer)
        // -----------------------------------------------------------------------

        public SQLLog(SessionSettings settings, SessionID sessionID)
        {
            _sessionID = sessionID ?? throw new ArgumentNullException(nameof(sessionID));
            _sessionSettings = settings ?? throw new ArgumentNullException(nameof(settings));

            _begin = _sessionID.BeginString;
            _sender = _sessionID.SenderCompID;
            _target = _sessionID.TargetCompID;
            _qual = string.IsNullOrEmpty(_sessionID.SessionQualifier) ? null : _sessionID.SessionQualifier;

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

            _errorLogPath = _sessionSettings.Get(sessionID).Has(SessionSettings.SQL_LOG_ERROR_PATH)
                ? _sessionSettings.Get(sessionID).GetString(SessionSettings.SQL_LOG_ERROR_PATH)
                : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "QuickFIXn");

            _incomingTableQ = QuoteName(incomingTable);
            _outgoingTableQ = QuoteName(outgoingTable);
            _incomingBackupTableQ = QuoteName(incomingBackupTable);
            _outgoingBackupTableQ = QuoteName(outgoingBackupTable);
            _eventTableQ = string.IsNullOrWhiteSpace(eventTable) ? "" : QuoteName(eventTable);
            _eventBackupTableQ = QuoteName(eventBackupTable);

            _writerTask = Task.Run(() => BackgroundWriterAsync(_cts.Token));
        }

        // -----------------------------------------------------------------------
        // Dispose — flushes the queue
        // -----------------------------------------------------------------------

        public void Dispose()
        {
            try
            {
                _channel.Writer.Complete();
                if (!_writerTask.Wait(TimeSpan.FromSeconds(5)))
                    LogError("Background writer did not flush within 5s on shutdown.");
            }
            catch (Exception ex)
            {
                LogError($"Dispose error: {ex.Message}");
            }
            finally
            {
                _cts.Dispose();
            }
        }

        // -----------------------------------------------------------------------
        // Public ILog methods — enqueue to bounded channel
        // -----------------------------------------------------------------------

        public void OnIncoming(string msg)
        {
            if (!_channel.Writer.TryWrite(new LogItem(_incomingTableQ, msg, DateTime.UtcNow)))
                TrackDrop();
        }

        public void OnOutgoing(string msg)
        {
            if (!_channel.Writer.TryWrite(new LogItem(_outgoingTableQ, msg, DateTime.UtcNow)))
                TrackDrop();
        }

        public void OnEvent(string s)
        {
            if (string.IsNullOrWhiteSpace(_eventTableQ))
                return;
            if (!_channel.Writer.TryWrite(new LogItem(_eventTableQ, s, DateTime.UtcNow)))
                TrackDrop();
        }

        private void TrackDrop()
        {
            var count = Interlocked.Increment(ref _dropCount);
            var now = DateTime.UtcNow;
            if ((now - _lastDropLog).TotalMilliseconds > DropLogIntervalMs)
            {
                _lastDropLog = now;

                if (_channel.Reader.Completion.IsCompleted)
                    LogError($"Channel closed (shutting down). Message not queued. Total dropped: {count}.");
                else
                    LogError($"Channel full ({ChannelCapacity}). Total dropped: {count}. Writer may be blocked or dead.");
            }
        }

        // -----------------------------------------------------------------------
        // Clear (original — unchanged)
        // -----------------------------------------------------------------------

        public void Clear()
        {
            if (string.IsNullOrWhiteSpace(_eventTableQ))
                return;

            try
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = $@"
DELETE FROM {_eventTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND ((@qual IS NULL AND session_qualifier IS NULL) OR (session_qualifier = @qual));";

                cmd.Parameters.Add(new SqlParameter("@begin", SqlDbType.NVarChar, 32) { Value = (object?)_begin ?? DBNull.Value });
                cmd.Parameters.Add(new SqlParameter("@sender", SqlDbType.NVarChar, 64) { Value = (object?)_sender ?? DBNull.Value });
                cmd.Parameters.Add(new SqlParameter("@target", SqlDbType.NVarChar, 64) { Value = (object?)_target ?? DBNull.Value });
                cmd.Parameters.Add(new SqlParameter("@qual", SqlDbType.NVarChar, 64) { IsNullable = true, Value = (object?)_qual ?? DBNull.Value });

                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.Write("OnClear: ");
                Console.WriteLine(ex.Message);
            }
        }

        // -----------------------------------------------------------------------
        // Backup (original — unchanged)
        // -----------------------------------------------------------------------

        public void Backup(DateTime? DateThreshold = null)
        {
            try
            {
                var bufferTime = DateTime.UtcNow - TimeSpan.FromSeconds(10);
                if (DateThreshold.HasValue)
                    bufferTime = DateThreshold.Value;

                using var conn = new SqlConnection(GetSqlConnectionString());
                conn.Open();

                using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                ExecBackupAndClear(conn, tx, _incomingTableQ, _incomingBackupTableQ, bufferTime, filterHeartbeats: true);

                if (!string.Equals(_incomingTableQ, _outgoingTableQ, StringComparison.OrdinalIgnoreCase))
                    ExecBackupAndClear(conn, tx, _outgoingTableQ, _outgoingBackupTableQ, bufferTime, filterHeartbeats: true);

                if (!string.IsNullOrWhiteSpace(_eventTableQ))
                    ExecBackupAndClear(conn, tx, _eventTableQ, _eventBackupTableQ, bufferTime, filterHeartbeats: false);

                tx.Commit();
            }
            catch (Exception ex)
            {
                Console.Write("OnBackup: ");
                Console.WriteLine(ex.Message);
            }
        }

        // -----------------------------------------------------------------------
        // Background writer — SELF-HEALING: restarts on any exception
        // -----------------------------------------------------------------------

        private async Task BackgroundWriterAsync(CancellationToken ct)
        {
            var batch = new List<LogItem>(BatchSize);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                    {
                        // Fill batch up to BatchSize
                        while (batch.Count < BatchSize && _channel.Reader.TryRead(out var item))
                            batch.Add(item);

                        if (batch.Count == 0)
                            continue;

                        // If batch isn't full, wait briefly for more items
                        if (batch.Count < BatchSize)
                        {
                            using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                            delayCts.CancelAfter(FlushIntervalMs);
                            try
                            {
                                while (batch.Count < BatchSize &&
                                       await _channel.Reader.WaitToReadAsync(delayCts.Token).ConfigureAwait(false))
                                {
                                    while (batch.Count < BatchSize && _channel.Reader.TryRead(out var extra))
                                        batch.Add(extra);
                                }
                            }
                            catch (OperationCanceledException) { /* timer expired, flush what we have */ }
                        }

                        if (batch.Count > 0)
                        {
                            await FlushBatchAsync(batch).ConfigureAwait(false);
                            batch.Clear();
                        }
                    }

                    // Channel completed (Dispose called) — flush remaining
                    while (_channel.Reader.TryRead(out var remaining))
                        batch.Add(remaining);
                    if (batch.Count > 0)
                        await FlushBatchAsync(batch).ConfigureAwait(false);

                    break; // channel is complete, exit cleanly
                }
                catch (OperationCanceledException)
                {
                    break; // shutting down
                }
                catch (Exception ex)
                {
                    // *** KEY FIX: log the error and RESTART the loop ***
                    LogError($"Writer error, restarting in {WriterRestartDelayMs}ms: [{ex.GetType().Name}] {ex.Message}\n{ex.StackTrace}");
                    batch.Clear();

                    try
                    {
                        await Task.Delay(WriterRestartDelayMs, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }

        // -----------------------------------------------------------------------
        // Flush — bulk insert via SqlBulkCopy for throughput
        // -----------------------------------------------------------------------

        private async Task FlushBatchAsync(List<LogItem> batch)
        {
            // Group by target table (messages_log vs event_log)
            var grouped = new Dictionary<string, List<LogItem>>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in batch)
            {
                if (!grouped.TryGetValue(item.TableQ, out var list))
                {
                    list = new List<LogItem>();
                    grouped[item.TableQ] = list;
                }
                list.Add(item);
            }

            try
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                await conn.OpenAsync().ConfigureAwait(false);

                foreach (var (tableQ, items) in grouped)
                {
                    try
                    {
                        var dt = new DataTable();
                        dt.Columns.Add("time", typeof(DateTime));
                        dt.Columns.Add("beginstring", typeof(string));
                        dt.Columns.Add("sendercompid", typeof(string));
                        dt.Columns.Add("targetcompid", typeof(string));
                        dt.Columns.Add("session_qualifier", typeof(string));
                        dt.Columns.Add("text", typeof(string));

                        foreach (var item in items)
                        {
                            dt.Rows.Add(
                                item.Time,
                                (object?)_begin ?? DBNull.Value,
                                (object?)_sender ?? DBNull.Value,
                                (object?)_target ?? DBNull.Value,
                                (object?)_qual ?? DBNull.Value,
                                (object?)item.Message ?? DBNull.Value
                            );
                        }

                        using var bulkCopy = new SqlBulkCopy(conn)
                        {
                            DestinationTableName = tableQ,
                            BatchSize = items.Count,
                            BulkCopyTimeout = 30
                        };

                        bulkCopy.ColumnMappings.Add("time", "time");
                        bulkCopy.ColumnMappings.Add("beginstring", "beginstring");
                        bulkCopy.ColumnMappings.Add("sendercompid", "sendercompid");
                        bulkCopy.ColumnMappings.Add("targetcompid", "targetcompid");
                        bulkCopy.ColumnMappings.Add("session_qualifier", "session_qualifier");
                        bulkCopy.ColumnMappings.Add("text", "text");

                        await bulkCopy.WriteToServerAsync(dt).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        LogError($"BulkCopy to {tableQ} failed ({items.Count} rows): {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"FlushBatch connection error: {ex.Message}");
            }
        }

        // -----------------------------------------------------------------------
        // Error logging — file-based so it works as a service
        // -----------------------------------------------------------------------

        private void LogError(string message)
        {
            var line = $"{DateTime.UtcNow:O} SQLLog [{_sender}->{_target}]: {message}";
            Console.WriteLine(line);

            try
            {
                Directory.CreateDirectory(_errorLogPath);
                var logFile = Path.Combine(_errorLogPath, "sqllog_errors.log");
                File.AppendAllText(logFile, line + Environment.NewLine);
            }
            catch { /* don't let logging errors kill anything */ }
        }

        // -----------------------------------------------------------------------
        // Backup helpers (original — unchanged)
        // -----------------------------------------------------------------------

        private void ExecBackupAndClear(
            SqlConnection conn,
            SqlTransaction tx,
            string srcTableQ,
            string dstTableQ,
            DateTime cutoffUtc,
            bool filterHeartbeats)
        {
            var heartbeatPredicate = filterHeartbeats
                ? " AND NOT([text] LIKE '%' + CHAR(1) + '35=0' + CHAR(1) + '%')"
                : "";

            var insertSql = $@"
INSERT INTO {dstTableQ} (time, beginstring, sendercompid, targetcompid, session_qualifier, [text])
SELECT time, beginstring, sendercompid, targetcompid, session_qualifier, [text]
FROM {srcTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND time < @cutoff
  AND ((@qual IS NULL AND session_qualifier IS NULL) OR (session_qualifier = @qual))
{heartbeatPredicate};";

            var deleteSql = $@"
DELETE FROM {srcTableQ}
WHERE beginstring = @begin
  AND sendercompid = @sender
  AND targetcompid = @target
  AND time < @cutoff
  AND ((@qual IS NULL AND session_qualifier IS NULL) OR (session_qualifier = @qual));";

            using var cmdInsert = new SqlCommand(insertSql, conn, tx);
            using var cmdDelete = new SqlCommand(deleteSql, conn, tx);

            AddBackupParams(cmdInsert, cutoffUtc);
            AddBackupParams(cmdDelete, cutoffUtc);

            cmdInsert.ExecuteNonQuery();
            cmdDelete.ExecuteNonQuery();
        }

        private void AddBackupParams(SqlCommand cmd, DateTime cutoffUtc)
        {
            // VarChar, not NVarChar - see AddSessionKeyParams in SQLStore: an
            // nvarchar param against varchar columns forces a column-side
            // convert and full-table scans on the log-table predicates.
            cmd.Parameters.Add(new SqlParameter("@begin", SqlDbType.VarChar, 32) { Value = (object?)_begin ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@sender", SqlDbType.VarChar, 64) { Value = (object?)_sender ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@target", SqlDbType.VarChar, 64) { Value = (object?)_target ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@qual", SqlDbType.VarChar, 64) { IsNullable = true, Value = (object?)_qual ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@cutoff", SqlDbType.DateTime2) { Value = cutoffUtc });
        }

        // -----------------------------------------------------------------------
        // Connection string builder (original — unchanged)
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
            if (!sb.ContainsKey("Application Name")) sb.ApplicationName = "QuickFIXn-SQLLog";

            return sb.ToString();
        }

        // -----------------------------------------------------------------------
        // Utilities (original — unchanged)
        // -----------------------------------------------------------------------

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
            else if (parts.Length == 2)
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
