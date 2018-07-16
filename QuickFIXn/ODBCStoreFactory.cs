using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace QuickFix
{
    public class ODBCStoreFactory : IMessageStoreFactory
    {

        private SessionSettings settings_;

        /// <summary>
        /// Create the factory with configuration in session settings
        /// </summary>
        /// <param name="settings"></param>
        public ODBCStoreFactory(SessionSettings settings)
        {
            settings_ = settings;
        }

        public IMessageStore Create(SessionID sessionID)
        {
            string user = string.Empty;
            string password = string.Empty;
            
            if(settings_.Get(sessionID).Has(SessionSettings.ODBC_STORE_USER))
                user = settings_.Get(sessionID).GetString(SessionSettings.ODBC_STORE_USER);

            if (settings_.Get(sessionID).Has(SessionSettings.ODBC_STORE_PASSWORD))
                password = settings_.Get(sessionID).GetString(SessionSettings.ODBC_STORE_PASSWORD);
            
            string connectionString = settings_.Get(sessionID).GetString(SessionSettings.ODBC_STORE_CONNECTION_STRING);

            return new ODBCStore(sessionID, user, password, connectionString);
        }
    }
}
