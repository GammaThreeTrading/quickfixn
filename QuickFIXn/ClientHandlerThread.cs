using System.Net.Sockets;
using System.Threading;
using System;
using QuickFix.Logger;

namespace QuickFix
{
    /// <summary>
    /// Created by a ThreadedSocketReactor to handle a client connection.
    /// Each ClientHandlerThread has a SocketReader which reads
    /// from the socket.
    /// </summary>
    internal class ClientHandlerThread : IResponder, IDisposable
    {
        internal class ExitedEventArgs : EventArgs
        {
            public ClientHandlerThread ClientHandlerThread { get; private set; }

            public ExitedEventArgs(ClientHandlerThread clientHandlerThread)
            {
                ClientHandlerThread = clientHandlerThread;
            }
        }

        internal delegate void ExitedEventHandler(object sender, ClientHandlerThread.ExitedEventArgs e);
        internal event ExitedEventHandler? Exited;

        public long Id { get; private set; }

        private Thread? _thread = null;
        private volatile bool _isShutdownRequested = false;

        // SocketReader is created in Run() AFTER the TLS handshake completes
        // on the worker thread. Constructing it in the ctor would run the
        // handshake on the reactor's accept thread, which is shared by every
        // session on this listener. A stalled handshake there freezes the
        // entire accept loop — port goes unreachable, only restart fixes it.
        private SocketReader? _socketReader = null;

        private readonly TcpClient _tcpClient;
        private readonly SocketSettings _socketSettings;
        private readonly AcceptorSocketDescriptor? _acceptorDescriptor;
        private readonly NonSessionLog _nonSessionLog;

        // Backstop against stalled TLS handshakes. Without this, a misbehaving
        // peer (or one whose TCP path got into a half-dead state) can leave
        // the worker blocked in AuthenticateAsServer indefinitely, leaking
        // worker threads. Applied as Socket.ReceiveTimeout for the duration
        // of the handshake only; the configured per-session timeout (or none)
        // is restored before normal session reads begin.
        private const int HandshakeReceiveTimeoutMs = 10000;

        internal ClientHandlerThread(
            TcpClient tcpClient,
            long clientId,
            SocketSettings socketSettings,
            AcceptorSocketDescriptor? acceptorDescriptor,
            NonSessionLog nonSessionLog
        ) {
            Id = clientId;
            _tcpClient = tcpClient;
            _socketSettings = socketSettings;
            _acceptorDescriptor = acceptorDescriptor;
            _nonSessionLog = nonSessionLog;
            // No I/O here. SocketReader (and the TLS handshake it triggers)
            // is constructed in Run() so the handshake runs on this worker
            // thread, not on the reactor's accept thread.
        }

        public void Start()
        {
            _thread = new Thread(Run);
            _thread.Start();
        }

        public void Shutdown(string reason)
        {
            // TODO - need the reason param?
            _isShutdownRequested = true;
        }

        public void Join()
        {
            if (_thread is null)
                return;
            if (_thread.IsAlive)
                _thread.Join(5000);
            _thread = null;
        }

        private void Run()
        {
            try
            {
                // TLS handshake happens here, on the worker thread, with a
                // receive timeout in place. If a peer connects but never
                // sends a valid ClientHello (or stalls mid-handshake), the
                // handshake fails after HandshakeReceiveTimeoutMs and only
                // this worker is affected — the reactor keeps accepting.
                int originalReceiveTimeout = _tcpClient.ReceiveTimeout;
                _tcpClient.ReceiveTimeout = HandshakeReceiveTimeoutMs;

                try
                {
                    _socketReader = new SocketReader(
                        _tcpClient, _socketSettings, this, _acceptorDescriptor, _nonSessionLog);
                }
                catch (Exception e)
                {
                    // Preserve the legacy log message format ("Error accepting
                    // connection: ...") so existing log analysis still works.
                    // Previously this was logged from ThreadedSocketReactor.Run()
                    // because the handshake ran on the accept thread; now it's
                    // logged from here, on the worker thread.
                    _nonSessionLog.OnEvent($"Error accepting connection: {e}");
                    try { _tcpClient.Close(); } catch { }
                    return;
                }

                // Restore the configured timeout (or 0 = no timeout) for
                // normal session reads, which need to block indefinitely
                // waiting for FIX messages.
                try { _tcpClient.ReceiveTimeout = originalReceiveTimeout; } catch { }

                while (!_isShutdownRequested)
                {
                    try
                    {
                        _socketReader.Read();
                    }
                    catch (Exception e)
                    {
                        Shutdown(e.Message);
                    }
                }
            }
            finally
            {
                // Always fire Exited, even on handshake failure, so the
                // reactor removes us from _clientThreads and we don't leak
                // entries.
                OnExited();
            }
        }

        private void OnExited() {
            Exited?.Invoke(this, new ExitedEventArgs(this));
        }

        #region Responder Members

        public bool Send(string data)
        {
            // _socketReader is null only during the brief window before the
            // TLS handshake completes. Send() should not be called in that
            // window (no Session is associated with this thread until after
            // a Logon has been read), but guard defensively.
            return _socketReader is not null && _socketReader.Send(data) > 0;
        }

        public void Disconnect()
        {
            Shutdown("Disconnected");
        }

        #endregion

        ~ClientHandlerThread() => Dispose(false);
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool _disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                if (_socketReader is not null)
                {
                    // Normal path: SocketReader owns the stream chain and
                    // closes the TcpClient when disposed.
                    _socketReader.Dispose();
                }
                else
                {
                    // Worker never created the SocketReader — either Start()
                    // was never called, or the handshake failed and Run()
                    // returned before SocketReader was constructed. Either
                    // way, the TcpClient is still our responsibility. Close
                    // it directly so the underlying socket doesn't leak
                    // until GC finalization.
                    try { _tcpClient.Close(); } catch { }
                }
            }
            _disposed = true;
        }
    }
}
