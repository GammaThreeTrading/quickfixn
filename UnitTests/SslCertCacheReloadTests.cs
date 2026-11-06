using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using NUnit.Framework;
using QuickFix;
using QuickFix.Logger;
using QuickFix.Transport;

namespace UnitTests
{
    /// <summary>
    /// SslCertCache used to hold a certificate for the life of the process once its path had been
    /// loaded, so a renewed PFX dropped over the old one was ignored until a restart. The cache now
    /// re-reads a certificate file when it changes on disk. These tests pin that, and pin that a
    /// bad replacement file never takes down a certificate that is already working.
    /// </summary>
    [TestFixture]
    public class SslCertCacheReloadTests
    {
        private const string Password = "test-only";
        private static readonly Oid ServerAuth = new("1.3.6.1.5.5.7.3.1");

        private string _dir = "";
        private DateTime _clock;

        [SetUp]
        public void SetUp()
        {
            // A fresh directory per test = a fresh cache key per test (the cache is static).
            _dir = Path.Combine(Path.GetTempPath(), "qfn-certcache-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_dir);
            _clock = DateTime.UtcNow.AddMinutes(-10);
        }

        [TearDown]
        public void TearDown()
        {
            try { Directory.Delete(_dir, true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
        }

        private static byte[] MakeServerPfx(string cn, out string thumbprint)
        {
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { ServerAuth }, true));
            using X509Certificate2 cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            thumbprint = cert.Thumbprint;
            return cert.Export(X509ContentType.Pfx, Password);
        }

        /// <summary>
        /// Write the file and give it a strictly newer timestamp than the previous write, so the
        /// tests do not depend on how fast two writes land on the file system clock.
        /// </summary>
        private void WriteFile(string path, byte[] bytes)
        {
            File.WriteAllBytes(path, bytes);
            _clock = _clock.AddSeconds(5);
            File.SetLastWriteTimeUtc(path, _clock);
        }

        [Test]
        public void UnchangedFile_ReturnsTheSameCachedInstance()
        {
            string path = Path.Combine(_dir, "server.pfx");
            WriteFile(path, MakeServerPfx("first", out string thumb));
            var log = new List<string>();

            X509Certificate2? a = SslCertCache.LoadCertificate(path, Password, log.Add);
            X509Certificate2? b = SslCertCache.LoadCertificate(path, Password, log.Add);

            Assert.That(a, Is.Not.Null);
            Assert.That(a!.Thumbprint, Is.EqualTo(thumb));
            Assert.That(b, Is.SameAs(a), "an unchanged file must not be re-read");
            Assert.That(log, Is.Empty);
        }

        [Test]
        public void ReplacedFile_IsPickedUpOnTheNextLoad()
        {
            string path = Path.Combine(_dir, "server.pfx");
            WriteFile(path, MakeServerPfx("first", out string oldThumb));
            var log = new List<string>();
            Assert.That(SslCertCache.LoadCertificate(path, Password, log.Add)!.Thumbprint, Is.EqualTo(oldThumb));

            WriteFile(path, MakeServerPfx("renewed", out string newThumb));
            X509Certificate2? renewed = SslCertCache.LoadCertificate(path, Password, log.Add);

            Assert.That(renewed!.Thumbprint, Is.EqualTo(newThumb));
            Assert.That(renewed.HasPrivateKey, Is.True);
            Assert.That(log, Has.Count.EqualTo(1));
            Assert.That(log[0], Does.Contain("changed on disk").And.Contain(newThumb).And.Contain(oldThumb));

            // ...and it is cached again until the next change.
            Assert.That(SslCertCache.LoadCertificate(path, Password, log.Add), Is.SameAs(renewed));
            Assert.That(log, Has.Count.EqualTo(1));
        }

        [Test]
        public void UnreadableReplacement_KeepsTheWorkingCertificate_AndRecoversWhenFixed()
        {
            string path = Path.Combine(_dir, "server.pfx");
            WriteFile(path, MakeServerPfx("first", out string oldThumb));
            var log = new List<string>();
            X509Certificate2? original = SslCertCache.LoadCertificate(path, Password, log.Add);

            // A half-copied / corrupt file lands on top of the good one.
            WriteFile(path, new byte[] { 0x30, 0x82, 0x01, 0x00, 0xde, 0xad });
            X509Certificate2? duringBadFile1 = SslCertCache.LoadCertificate(path, Password, log.Add);
            X509Certificate2? duringBadFile2 = SslCertCache.LoadCertificate(path, Password, log.Add);

            Assert.That(duringBadFile1, Is.SameAs(original));
            Assert.That(duringBadFile2, Is.SameAs(original));
            Assert.That(log, Has.Count.EqualTo(1), "the same bad file is reported once, not once per connection");
            Assert.That(log[0], Does.Contain("could not be loaded").And.Contain(oldThumb));

            // The copy completes with a good file.
            WriteFile(path, MakeServerPfx("renewed", out string newThumb));
            Assert.That(SslCertCache.LoadCertificate(path, Password, log.Add)!.Thumbprint, Is.EqualTo(newThumb));
            Assert.That(log, Has.Count.EqualTo(2));
        }

        [Test]
        public void WrongPasswordReplacement_KeepsTheWorkingCertificate()
        {
            string path = Path.Combine(_dir, "server.pfx");
            WriteFile(path, MakeServerPfx("first", out string oldThumb));
            var log = new List<string>();
            X509Certificate2? original = SslCertCache.LoadCertificate(path, Password, log.Add);

            // The renewed PFX was exported with a different password than the session is configured with.
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest("CN=other-password", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using X509Certificate2 other = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
            WriteFile(path, other.Export(X509ContentType.Pfx, "a-different-password"));

            Assert.That(SslCertCache.LoadCertificate(path, Password, log.Add), Is.SameAs(original));
            Assert.That(log, Has.Count.EqualTo(1));
            Assert.That(log[0], Does.Contain("could not be loaded").And.Contain(oldThumb));
        }

        [Test]
        public void FileMissingForAMoment_KeepsTheWorkingCertificate()
        {
            string path = Path.Combine(_dir, "server.pfx");
            WriteFile(path, MakeServerPfx("first", out _));
            var log = new List<string>();
            X509Certificate2? original = SslCertCache.LoadCertificate(path, Password, log.Add);

            File.Delete(path);   // delete-then-copy style swap, caught in between

            Assert.That(SslCertCache.LoadCertificate(path, Password, log.Add), Is.SameAs(original));
            Assert.That(log, Is.Empty);
        }

        /// <summary>
        /// The end-to-end shape: the acceptor path (SslStreamFactory.CreateServerStreamAndAuthenticate,
        /// one call per accepted socket) presents whatever is on disk at connect time. Two real TLS
        /// handshakes over loopback with the PFX replaced in between and no new settings object.
        /// </summary>
        [Test]
        public void AcceptorHandshake_PresentsTheRenewedCertificate_WithoutNewSettings()
        {
            string path = Path.Combine(_dir, "wildcard.pfx");
            WriteFile(path, MakeServerPfx("first", out string oldThumb));

            var dict = new SettingsDictionary();
            dict.SetBool(SessionSettings.SSL_ENABLE, true);
            dict.SetString(SessionSettings.SSL_CERTIFICATE, path);
            dict.SetString(SessionSettings.SSL_CERTIFICATE_PASSWORD, Password);
            dict.SetBool(SessionSettings.SSL_VALIDATE_CERTIFICATES, false);
            dict.SetBool(SessionSettings.SSL_REQUIRE_CLIENT_CERTIFICATE, false);
            var settings = new SocketSettings();
            settings.Configure(dict);   // captured once, as at instance start

            using var nonSessionLog = new NonSessionLog(new ScreenLogFactory(false, false, false));
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                int port = ((IPEndPoint)listener.LocalEndpoint).Port;

                Assert.That(HandshakeAndGetServerThumbprint(listener, port, settings, nonSessionLog), Is.EqualTo(oldThumb));

                WriteFile(path, MakeServerPfx("renewed", out string newThumb));

                Assert.That(HandshakeAndGetServerThumbprint(listener, port, settings, nonSessionLog), Is.EqualTo(newThumb));
            }
            finally
            {
                listener.Stop();
            }
        }

        private static string HandshakeAndGetServerThumbprint(
            TcpListener listener, int port, SocketSettings settings, NonSessionLog nonSessionLog)
        {
            Task serverSide = Task.Run(() =>
            {
                using TcpClient accepted = listener.AcceptTcpClient();
                using Stream tls = new SslStreamFactory(settings, nonSessionLog)
                    .CreateServerStreamAndAuthenticate(accepted.GetStream());
                tls.WriteByte(1);   // complete the handshake from the server's point of view
                tls.Flush();
            });

            string? presented = null;
            using (var client = new TcpClient())
            {
                client.Connect(IPAddress.Loopback, port);
                using var ssl = new SslStream(client.GetStream(), false, (_, cert, _, _) =>
                {
                    presented = cert is null ? null : new X509Certificate2(cert).Thumbprint;
                    return true;   // self-signed test certificate
                });
                ssl.AuthenticateAsClient("localhost");
                Assert.That(ssl.ReadByte(), Is.EqualTo(1));
            }

            Assert.That(serverSide.Wait(TimeSpan.FromSeconds(15)), Is.True, "server side of the handshake did not finish");
            Assert.That(presented, Is.Not.Null);
            return presented!;
        }
    }
}
