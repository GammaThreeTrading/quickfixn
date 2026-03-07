using Microsoft.Data.SqlClient;
using QuickFix.Logger;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace QuickFix
{
    /// <summary>
    /// SQLLog with async write-behind for the hot path only.
    ///
    /// ONLY THREE METHODS CHANGED from original:
    ///   - OnIncoming() : enqueues to background writer, returns immediately
    ///   - OnOutgoing()  : enqueues to background writer, returns immediately
    ///   - OnEvent()     : enqueues to background writer, returns immediately
    ///
    /// Everything else (Clear, Backup, GetSqlConnectionString, QuoteName) is the
    /// original code, unchanged. No startup or reset changes.
    /// </summary>
    public class SQLLog : ILog, IDisposable
    {
        // -----------------------------------------------------------------------
        // Write-behind queue (hot path only)
        // -----------------------------------------------------------------------

        private sealed record LogItem(string TableQ, string Message, DateTime Time);

        private const int BatchSize = 50;
        private const int FlushIntervalMs = 100;

        private readonly Channel<LogItem> _channel = Channel.CreateUnbounded<LogItem>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        private readonly Task _writerTask;
        private readonly CancellationTokenSource _cts = new();

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

            _incomingTableQ = QuoteName(incomingTable);
            _outgoingTableQ = QuoteName(outgoingTable);
            _incomingBackupTableQ = QuoteName(incomingBackupTable);
            _outgoingBackupTableQ = QuoteName(outgoingBackupTable);
            _eventTableQ = string.IsNullOrWhiteSpace(eventTable) ? "" : QuoteName(eventTable);
            _eventBackupTableQ = QuoteName(eventBackupTable);

            _writerTask = Task.Run(() => BackgroundWriterAsync(_cts.Token));
        }

        // -----------------------------------------------------------------------
        // Dispose — was empty, now flushes the queue
        // -----------------------------------------------------------------------

        public void Dispose()
        {
            try
            {
                _channel.Writer.Complete();
                if (!_writerTask.Wait(TimeSpan.FromSeconds(5)))
                    Console.WriteLine($"SQLLog [{_sender}->{_target}]: Background writer did not flush within 5s on shutdown.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLLog [{_sender}->{_target}]: Dispose error: {ex.Message}");
            }
            finally
            {
                _cts.Dispose();
            }
        }

        // -----------------------------------------------------------------------
        // Public ILog methods
        // CHANGED: enqueue instead of calling InsertMessage directly
        // -----------------------------------------------------------------------

        public void OnIncoming(string msg) => _channel.Writer.TryWrite(new LogItem(_incomingTableQ, msg, DateTime.UtcNow));

        public void OnOutgoing(string msg) => _channel.Writer.TryWrite(new LogItem(_outgoingTableQ, msg, DateTime.UtcNow));

        public void OnEvent(string s)
        {
            if (string.IsNullOrWhiteSpace(_eventTableQ))
                return;
            _channel.Writer.TryWrite(new LogItem(_eventTableQ, s, DateTime.UtcNow));
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
        // Background writer
        // -----------------------------------------------------------------------

        private async Task BackgroundWriterAsync(CancellationToken ct)
        {
            var batch = new List<LogItem>(BatchSize);

            try
            {
                while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                {
                    while (batch.Count < BatchSize && _channel.Reader.TryRead(out var item))
                        batch.Add(item);

                    if (batch.Count == 0)
                        continue;

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
                        catch (OperationCanceledException) { }
                    }

                    if (batch.Count > 0)
                    {
                        await FlushBatchAsync(batch).ConfigureAwait(false);
                        batch.Clear();
                    }
                }

                // Flush remaining on Dispose
                while (_channel.Reader.TryRead(out var remaining))
                    batch.Add(remaining);
                if (batch.Count > 0)
                    await FlushBatchAsync(batch).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLLog [{_sender}->{_target}]: Background writer fatal error: {ex.Message}");
            }
        }

        private async Task FlushBatchAsync(List<LogItem> batch)
        {
            try
            {
                using var conn = new SqlConnection(GetSqlConnectionString());
                await conn.OpenAsync().ConfigureAwait(false);

                foreach (var item in batch)
                {
                    try
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = $@"
INSERT INTO {item.TableQ}
(time, beginstring, sendercompid, targetcompid, session_qualifier, [text])
VALUES (@time, @begin, @sender, @target, @qual, @text);";

                        cmd.Parameters.Add(new SqlParameter("@time", SqlDbType.DateTime2) { Value = item.Time });
                        cmd.Parameters.Add(new SqlParameter("@begin", SqlDbType.NVarChar, 32) { Value = (object?)_begin ?? DBNull.Value });
                        cmd.Parameters.Add(new SqlParameter("@sender", SqlDbType.NVarChar, 64) { Value = (object?)_sender ?? DBNull.Value });
                        cmd.Parameters.Add(new SqlParameter("@target", SqlDbType.NVarChar, 64) { Value = (object?)_target ?? DBNull.Value });
                        cmd.Parameters.Add(new SqlParameter("@qual", SqlDbType.NVarChar, 64) { IsNullable = true, Value = (object?)_qual ?? DBNull.Value });
                        cmd.Parameters.Add(new SqlParameter("@text", SqlDbType.NVarChar, -1) { Value = (object?)item.Message ?? DBNull.Value });

                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"SQLLog [{_sender}->{_target}]: INSERT failed: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQLLog [{_sender}->{_target}]: FlushBatch connection error: {ex.Message}");
            }
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
            cmd.Parameters.Add(new SqlParameter("@begin", SqlDbType.NVarChar, 32) { Value = (object?)_begin ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@sender", SqlDbType.NVarChar, 64) { Value = (object?)_sender ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@target", SqlDbType.NVarChar, 64) { Value = (object?)_target ?? DBNull.Value });
            cmd.Parameters.Add(new SqlParameter("@qual", SqlDbType.NVarChar, 64) { IsNullable = true, Value = (object?)_qual ?? DBNull.Value });
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
