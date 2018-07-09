using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QuickFix
{
    public class ODBCLogFactory : ILogFactory
    {

        SessionSettings settings_;

        #region LogFactory Members

        public ODBCLogFactory(SessionSettings settings)
        {
            settings_ = settings;
        }

        public ILog Create(SessionID sessionID)
        {
            return new ODBCLog(settings_, sessionID);
        }

        #endregion

    }
}
