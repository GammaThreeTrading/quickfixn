using QuickFix.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        #endregion
    }
}
