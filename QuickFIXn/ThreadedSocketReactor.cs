using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System;
using QuickFix.Logger;

namespace QuickFix
{
    // TODO v2.0 - consider changing to internal

    /// <summary>
    /// Handles incoming connections on a single endpoint. When a socket connection
    /// is accepted, a ClientHandlerThread is created to handle the connection
    /// </summary>
    public class ThreadedSocketReactor
    {
        public enum State { RUNNING, SHUTDOWN_REQUESTED, SHUTDOWN_COMPLETE }

        private const int ClientThreadJoinTimeoutMs = 5000;
        private const int ServerThreadJoinTimeoutMs = 10000;

        public State ReactorState
        {
            get { lock (_sync) { return _state; } }
        }

        private readonly object _sync = new();
        private State _state = State.RUNNING;
        private long _nextClientId = 0;
        private Thread? _serverThread = null;
        private readonly Dictionary<long, ClientHandlerThread> _clientThreads = new();
        private readonly TcpListener _tcpListener;
        private readonly SocketSettings _socketSettings;
        private readonly IPEndPoint _serverSocketEndPoint;
        private readonly AcceptorSocketDescriptor? _acceptorSocketDescriptor;
        private readonly NonSessionLog _nonSessionLog;

        internal ThreadedSocketReactor(
            IPEndPoint serverSocketEndPoint,
            SocketSettings socketSettings,
            AcceptorSocketDescriptor? acceptorSocketDescriptor,
            NonSessionLog nonSessionLog)
        {
            _socketSettings = socketSettings;
            _serverSocketEndPoint = serverSocketEndPoint;
            _tcpListener = new TcpListener(_serverSocketEndPoint);
            _acceptorSocketDescriptor = acceptorSocketDescriptor;
            _nonSessionLog = nonSessionLog;
        }

        public void Start()
        {
            lock (_sync)
            {
                if (_state == State.RUNNING && _serverThread is null)
                {
                    if (State.SHUTDOWN_REQUESTED != _state)
                    {
                        try
                        {
                            _tcpListener.Start();
                        }
                        catch (Exception e)
                        {
                            LogError("Error starting listener", e);
                            throw;
                        }
                    }
                    _serverThread = new Thread(Run);
                    _serverThread.Start();
                }
            }
        }

        public void Shutdown()
        {
            Thread? serverThread;

            lock (_sync)
            {
                if (State.RUNNING != _state)
                    return;

                _state = State.SHUTDOWN_REQUESTED;
                serverThread = _serverThread;

                // Close the listener directly — this unblocks AcceptTcpClient()
                // immediately by causing it to throw a SocketException.
                // Much more reliable than the old "killer connection" trick,
                // which could fail if loopback was blocked or the listener
                // was bound to a non-loopback address.
                try
                {
                    _tcpListener.Stop();
                }
                catch (Exception e)
                {
                    LogError("Error stopping listener during shutdown", e);
                }
            }

            // Wait for the server thread OUTSIDE the lock.
            // Run() needs to acquire _sync during ShutdownClientHandlerThreads(),
            // so holding the lock here would deadlock.
            if (serverThread != null)
            {
                if (!serverThread.Join(ServerThreadJoinTimeoutMs))
                {
                    LogError($"Server thread did not exit within {ServerThreadJoinTimeoutMs}ms");
                }
            }
        }

        public void Run()
        {
            while (State.RUNNING == ReactorState)
            {
                try
                {
                    TcpClient client = _tcpListener.AcceptTcpClient();
                    if (State.RUNNING == ReactorState)
                    {
                        ApplySocketOptions(client, _socketSettings);
                        ClientHandlerThread t = new ClientHandlerThread(
                            client, _nextClientId++, _socketSettings, _acceptorSocketDescriptor, _nonSessionLog);
                        t.Exited += OnClientHandlerThreadExited;
                        lock (_sync)
                        {
                            _clientThreads.Add(t.Id, t);
                        }

                        t.Start();
                    }
                    else
                    {
                        client.Close();
                    }
                }
                catch (Exception e)
                {
                    if (State.RUNNING == ReactorState)
                        LogError("Error accepting connection", e);
                }
            }

            // Listener is already stopped by Shutdown(), but call these
            // defensively in case Run() exits for another reason.
            try { _tcpListener.Server.Close(); } catch { }
            try { _tcpListener.Stop(); } catch { }

            ShutdownClientHandlerThreads();
        }

        internal void OnClientHandlerThreadExited(object sender, ClientHandlerThread.ExitedEventArgs e)
        {
            lock (_sync)
            {
                if (_clientThreads.TryGetValue(e.ClientHandlerThread.Id, out var t))
                {
                    _clientThreads.Remove(t.Id);
                    t.Dispose();
                }
            }
        }

        /// <summary>
        /// Apply socket options from settings
        /// </summary>
        /// <param name="client"></param>
        /// <param name="socketSettings"></param>
        public static void ApplySocketOptions(TcpClient client, SocketSettings socketSettings)
        {
            client.LingerState = new LingerOption(false, 0);
            client.NoDelay = socketSettings.SocketNodelay;
            if (socketSettings.SocketReceiveBufferSize.HasValue)
            {
                client.ReceiveBufferSize = socketSettings.SocketReceiveBufferSize.Value;
            }
            if (socketSettings.SocketSendBufferSize.HasValue)
            {
                client.SendBufferSize = socketSettings.SocketSendBufferSize.Value;
            }
            if (socketSettings.SocketReceiveTimeout.HasValue)
            {
                client.ReceiveTimeout = socketSettings.SocketReceiveTimeout.Value;
            }
            if (socketSettings.SocketSendTimeout.HasValue)
            {
                client.SendTimeout = socketSettings.SocketSendTimeout.Value;
            }
        }

        private void ShutdownClientHandlerThreads()
        {
            lock (_sync)
            {
                if (State.SHUTDOWN_COMPLETE != _state)
                {
                    foreach (ClientHandlerThread t in _clientThreads.Values)
                    {
                        t.Exited -= OnClientHandlerThreadExited;
                        t.Shutdown("reactor is shutting down");
                        try
                        {
                            if (!t.Join(ClientThreadJoinTimeoutMs))
                            {
                                LogError($"ClientHandlerThread {t.Id} did not exit within {ClientThreadJoinTimeoutMs}ms, abandoning");
                            }
                        }
                        catch (Exception e)
                        {
                            LogError("Error shutting down client handler thread", e);
                        }
                        t.Dispose();
                    }
                    _clientThreads.Clear();
                    _state = State.SHUTDOWN_COMPLETE;
                }
            }
        }

        /// <summary>
        /// Write to the NonSessionLog
        /// </summary>
        /// <param name="s"></param>
        /// <param name="ex"></param>
        private void LogError(string s, Exception? ex = null)
        {
            _nonSessionLog.OnEvent(ex is null ? $"{s}" : $"{s}: {ex}");
        }
    }
}
