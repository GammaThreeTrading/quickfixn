

using QuickFix.Logger;
using System.Runtime;

namespace QuickFix
{
    public class SQLLogFactory : ILogFactory
    {
        SessionSettings settings_;

        #region LogFactory Members

        public SQLLogFactory(SessionSettings settings)
        {
            settings_ = settings;
        }

        public ILog Create(SessionID sessionID)
        {
            return new SQLLog(settings_, sessionID);
        }

        public ILog CreateNonSessionLog()
        {
            return new NullLog();
             //return new SQLLog(settings_, new SessionID("Non", "Session", "Log"));
        }

        #endregion
    }
}
