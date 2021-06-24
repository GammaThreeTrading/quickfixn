using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickFix
{
    public class ODBCFileHybridLogFactory : ILogFactory
    {

        SessionSettings settings_;

        #region LogFactory Members

        public ODBCFileHybridLogFactory(SessionSettings settings)
        {
            settings_ = settings;
        }

        public ILog Create(SessionID sessionID)
        {
            return new ODBCFileHybridLog(settings_, sessionID);
        }

        #endregion

    }
}
