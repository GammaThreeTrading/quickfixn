using System;
using System.Buffers.Binary;
using System.IO;

namespace QuickFix
{
    /// <summary>
    /// Tiny per-session seqnum journal: a fixed 80-byte local file that makes
    /// sequence numbers durable against process death at local-disk cost
    /// (microseconds) instead of a per-message SQL round trip.
    ///
    /// Durability model: Write() returns after the bytes are handed to the OS
    /// (FileStream.Flush). That survives kill/crash/OOM of the process - the
    /// OS completes buffered writes even when the writing process is gone.
    /// It does NOT survive loss of the whole machine; SQLStore covers that by
    /// mirroring seqnums to the sessions table asynchronously and taking the
    /// per-direction MAX of journal and SQL on startup.
    ///
    /// Torn-write safety: two 40-byte slots written alternately, each carrying
    /// a monotonically increasing counter and an XOR checksum. A write torn by
    /// a crash corrupts at most the slot being written; the other slot still
    /// holds the previous consistent state.
    ///
    /// Slot layout (5 x int64, little endian):
    ///   [0] counter   [1] creationTimeTicks   [2] sender   [3] target   [4] checksum
    ///
    /// The creationTimeTicks stamp ties the journal to a session epoch: after
    /// a Reset (new creation time) or against a different session day, a stale
    /// journal fails the epoch check and is ignored.
    ///
    /// Not thread-safe by itself: SQLStore only calls it under _storeLock.
    /// </summary>
    public sealed class SeqNumJournal : IDisposable
    {
        private const int SlotSize = 40;
        private const int FileSize = SlotSize * 2;
        private const long ChecksumMagic = unchecked((long)0x5EBA11AD5EBA11AD);

        private readonly FileStream _stream;
        private readonly byte[] _buffer = new byte[SlotSize];
        private long _counter;

        public string Path { get; }

        public SeqNumJournal(string directory, string sessionKey)
        {
            Directory.CreateDirectory(directory);

            var safeName = Sanitize(sessionKey) + ".seqjournal";
            Path = System.IO.Path.Combine(directory, safeName);

            _stream = new FileStream(
                Path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
                bufferSize: FileSize);

            if (_stream.Length < FileSize)
                _stream.SetLength(FileSize);
        }

        /// <summary>
        /// Reads the newest valid slot. Returns false if the file is empty,
        /// corrupt, or has never been written.
        /// </summary>
        public bool TryRead(out long creationTimeTicks, out ulong sender, out ulong target)
        {
            creationTimeTicks = 0; sender = 0; target = 0;

            Span<byte> all = stackalloc byte[FileSize];
            _stream.Seek(0, SeekOrigin.Begin);
            if (_stream.Read(all) != FileSize)
                return false;

            long bestCounter = -1;
            for (int slot = 0; slot < 2; slot++)
            {
                var s = all.Slice(slot * SlotSize, SlotSize);
                long counter = BinaryPrimitives.ReadInt64LittleEndian(s);
                long ticks = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(8));
                long snd = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(16));
                long tgt = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(24));
                long check = BinaryPrimitives.ReadInt64LittleEndian(s.Slice(32));

                if (counter <= 0)
                    continue;
                if (check != (counter ^ ticks ^ snd ^ tgt ^ ChecksumMagic))
                    continue;

                if (counter > bestCounter)
                {
                    bestCounter = counter;
                    creationTimeTicks = ticks;
                    sender = (ulong)snd;
                    target = (ulong)tgt;
                }
            }

            if (bestCounter > 0)
            {
                _counter = bestCounter;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Persists both seqnums for the given session epoch. Synchronous;
        /// returns once the OS owns the bytes.
        /// </summary>
        public void Write(long creationTimeTicks, ulong sender, ulong target)
        {
            _counter++;
            int slot = (int)(_counter % 2);

            var s = _buffer.AsSpan();
            BinaryPrimitives.WriteInt64LittleEndian(s, _counter);
            BinaryPrimitives.WriteInt64LittleEndian(s.Slice(8), creationTimeTicks);
            BinaryPrimitives.WriteInt64LittleEndian(s.Slice(16), (long)sender);
            BinaryPrimitives.WriteInt64LittleEndian(s.Slice(24), (long)target);
            BinaryPrimitives.WriteInt64LittleEndian(s.Slice(32),
                _counter ^ creationTimeTicks ^ (long)sender ^ (long)target ^ ChecksumMagic);

            _stream.Seek(slot * SlotSize, SeekOrigin.Begin);
            _stream.Write(_buffer, 0, SlotSize);
            _stream.Flush();   // hand to the OS; survives process death
        }

        private static string Sanitize(string name)
        {
            var invalid = System.IO.Path.GetInvalidFileNameChars();
            var chars = name.ToCharArray();
            for (int i = 0; i < chars.Length; i++)
            {
                if (Array.IndexOf(invalid, chars[i]) >= 0)
                    chars[i] = '_';
            }
            return new string(chars);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
