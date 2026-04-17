using QuickFix.Logger;
using System;
using System.IO;

namespace QuickFix
{
    public class SQLLogFactory : ILogFactory
    {
        SessionSettings settings_;
        string instanceName_;
        string logPath_;

        #region LogFactory Members

        public SQLLogFactory(
            SessionSettings settings,
            string instanceName = "QuickFIXN",
            string? logPath = null)
        {
            settings_ = settings;
            instanceName_ = SanitizeForFilename(instanceName);
            logPath_ = logPath ?? Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "QuickFIXN");
        }

        public ILog Create(SessionID sessionID)
        {
            return new SQLLog(settings_, sessionID);
        }

        public ILog CreateNonSessionLog()
        {
            try
            {
                return new SafeLog(new FileLog(logPath_, new SessionID("Non", "Session", instanceName_)));
            }
            catch
            {
                return new NullLog();
            }
        }

        private static string SanitizeForFilename(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return "QuickFIXN";
            foreach (char c in Path.GetInvalidFileNameChars())
                s = s.Replace(c, '_');
            return s;
        }

        #endregion

        private sealed class SafeLog : ILog
        {
            private readonly ILog _inner;
            public SafeLog(ILog inner) { _inner = inner; }
            public void Clear() { try { _inner.Clear(); } catch { } }
            public void OnIncoming(string msg) { try { _inner.OnIncoming(msg); } catch { } }
            public void OnOutgoing(string msg) { try { _inner.OnOutgoing(msg); } catch { } }
            public void OnEvent(string s) { try { _inner.OnEvent(s); } catch { } }
            public void Dispose() { try { _inner.Dispose(); } catch { } }
        }

        private sealed class NullLog : ILog
        {
            public void Clear() { }
            public void OnIncoming(string msg) { }
            public void OnOutgoing(string msg) { }
            public void OnEvent(string s) { }
            public void Dispose() { }
        }
    }
}