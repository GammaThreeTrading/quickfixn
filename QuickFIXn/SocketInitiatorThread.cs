using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using QuickFix.Logger;

namespace QuickFix;

/// <summary>
/// Handles a connection with an acceptor.
/// </summary>
public class SocketInitiatorThread : IResponder
{
    public Session Session { get; }
    public Transport.SocketInitiator Initiator { get; }
    public NonSessionLog NonSessionLog { get; }

    public const int BUF_SIZE = 512;

    private Thread? _thread;
    private readonly byte[] _readBuffer = new byte[BUF_SIZE];
    private readonly Parser _parser = new();
    // volatile: written by Connect() on the initiator thread, read by Send() on the session thread.
    private volatile Stream? _stream;
    private readonly CancellationTokenSource _readCancellationTokenSource = new();
    private readonly IPEndPoint _socketEndPoint;
    private readonly SocketSettings _socketSettings;
    // volatile: written by Disconnect() (any thread), read by Read() on the reader thread.
    // Without volatile, the reader thread may cache a stale false value and never exit.
    private volatile bool _isDisconnectRequested = false;
    // Used by Interlocked.Exchange to ensure Disconnect() body executes only once,
    // preventing ObjectDisposedException on the CTS from a concurrent second call.
    private int _disconnectCalled = 0;

    /// <summary>
    /// Keep a task for handling async read
    /// </summary>
    private Task<int>? _currentReadTask;

    public SocketInitiatorThread(
        Transport.SocketInitiator initiator,
        Session session,
        IPEndPoint socketEndPoint,
        SocketSettings socketSettings,
        NonSessionLog nonSessionLog)
    {
        Initiator = initiator;
        Session = session;
        NonSessionLog = nonSessionLog;
        _socketEndPoint = socketEndPoint;
        _socketSettings = socketSettings;
    }

    public void Start()
    {
        _isDisconnectRequested = false;
        _thread = new Thread(Transport.SocketInitiator.SocketInitiatorThreadStart);
        _thread.Start(this);
    }

    public void Join()
    {
        if (_thread is null)
            return;
        Disconnect();
        // Make sure session's socket reader thread doesn't try to do a Join on itself!
        if (Environment.CurrentManagedThreadId != _thread.ManagedThreadId)
            _thread.Join(2000);
        _thread = null;
    }

    public void Connect()
    {
        Debug.Assert(_stream == null);

        // Set up the stream first (network call)
        var stream = SetupStream();

        // Guard: if Disconnect() was called while SetupStream() was in progress,
        // don't install the responder — the session has already moved on.
        // Without this guard, SetResponder() installs a responder on a session that
        // just disconnected, causing Send() to throw SocketException 10058 on a
        // dead stream and the session never recovers.
        if (_isDisconnectRequested)
        {
            stream.Close();
            return;
        }

        _stream = stream;
        Session.SetResponder(this);
    }

    /// <summary>
    /// Setup/Connect to the other party.
    /// Override this in order to setup other types of streams with other settings
    /// </summary>
    /// <returns>Stream representing the (network)connection to the other party</returns>
    protected virtual Stream SetupStream()
    {
        return Transport.StreamFactory.CreateClientStream(_socketEndPoint, _socketSettings, NonSessionLog);
    }

    public bool Read()
    {
        try
        {
            int bytesRead = ReadSome(_readBuffer, 1000);
            if (bytesRead > 0)
                _parser.AddToStream(_readBuffer, bytesRead);
            else
                Session.Next();

            ProcessStream();
            return true;
        }
        catch (ObjectDisposedException)
        {
            // this exception means _socket is already closed when poll() is called
            if (_isDisconnectRequested == false)
                Disconnect();
        }
        catch (Exception e)
        {
            Session.Log.OnEvent(e.ToString());
            Disconnect();
        }
        return false;
    }

    /// <summary>
    /// Reads data from the network into the specified buffer.
    /// It will wait up to the specified number of milliseconds for data to arrive,
    /// if no data has arrived after the specified number of milliseconds then the function returns 0
    /// </summary>
    /// <param name="buffer">The buffer.</param>
    /// <param name="timeoutMilliseconds">The timeout milliseconds.</param>
    /// <returns>The number of bytes read into the buffer</returns>
    /// <exception cref="System.Net.Sockets.SocketException">On connection reset</exception>
    protected virtual int ReadSome(byte[] buffer, int timeoutMilliseconds)
    {
        if (_stream is null)
        {
            throw new ApplicationException("Initiator is not connected (uninitialized stream)");
        }

        // NOTE: FROM HERE, THIS FUNCTION IS EXACTLY THE SAME AS THE ONE IN SocketReader.
        // Any changes made here should also be performed there.
        try
        {
            // Begin read if it is not already started
            _currentReadTask ??= _stream.ReadAsync(buffer, 0, buffer.Length, _readCancellationTokenSource.Token);

            if (_currentReadTask.Wait(timeoutMilliseconds))
            {
                // Dispose/nullify currentReadTask *before* retrieving .Result.
                //   Accessing .Result can throw an exception, so we need to reset currentReadTask
                //   first, to set us up for the next read even if an exception is thrown.
                Task<int>? request = _currentReadTask;
                _currentReadTask = null;

                int bytesRead = request.Result; // (As mentioned above, this can throw an exception!)
                if (0 == bytesRead)
                    throw new SocketException(Convert.ToInt32(SocketError.Shutdown));

                return bytesRead;
            }

            return 0;
        }
        catch (AggregateException ex) // Timeout
        {
            _currentReadTask = null;

            if (ex.InnerException is OperationCanceledException)
            {
                // Nothing read 
                return 0;
            }

            var ioException = ex.InnerException as IOException;
            var inner = ioException?.InnerException as SocketException;
            if (inner is not null && inner.SocketErrorCode == SocketError.TimedOut)
            {
                // Nothing read 
                return 0;
            }

            if (inner is not null)
            {
                throw inner; //rethrow SocketException part (which we have exception logic for)
            }

            throw; //rethrow original exception
        }
    }

    private void ProcessStream()
    {
        while (_parser.ReadFixMessage(out var msg))
        {
            Session.Next(msg);
        }
    }

    #region Responder Members

    public bool Send(string data)
    {
        if (_stream is null)
        {
            throw new ApplicationException("Initiator is not connected (uninitialized stream)");
        }

        byte[] rawData = CharEncoding.GetBytes(data);
        _stream.Write(rawData, 0, rawData.Length);
        return true;
    }

    public void Disconnect()
    {
        _isDisconnectRequested = true;

        // Guard against concurrent or repeated calls. The CTS can only be cancelled
        // and disposed once — a second call would throw ObjectDisposedException, which
        // previously propagated out and prevented SetDisconnected() from being called,
        // leaving the session permanently stuck in _pending.
        if (Interlocked.Exchange(ref _disconnectCalled, 1) != 0)
            return;

        try
        {
            _readCancellationTokenSource.Cancel();

            // Wait for the in-flight read task to complete BEFORE disposing the CTS.
            // Disposing the CTS while ReadAsync is still running causes ObjectDisposedException
            // when the task tries to access the token.
            _currentReadTask?.ContinueWith(_ => { }).Wait(1000);
            _currentReadTask?.Dispose();
            _currentReadTask = null;

            // Now safe to dispose — no tasks are using the token anymore.
            _readCancellationTokenSource.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // CTS was already disposed — safe to continue to stream close
        }
        finally
        {
            _stream?.Close();
        }
    }

    #endregion
}

