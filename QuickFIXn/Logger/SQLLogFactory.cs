

using QuickFix.Logger;

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
            throw new System.NotImplementedException();
        }

        #endregion
    }
}
