using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using QuickFix.Transport;

namespace UnitTests
{
    /// <summary>
    /// SslStreamFactory.VerifyRemoteCertificate builds the remote chain with
    /// AllowUnknownCertificateAuthority (the configured SSLCACertificate is a private anchor, not in the
    /// OS trust store). That flag also masks PartialChain, so Build() alone accepts a leaf from ANY issuer.
    /// ChainIsAnchoredBy is the check that makes SSLCACertificate mean something; these tests pin it.
    /// </summary>
    [TestFixture]
    public class SslStreamFactoryChainTests
    {
        private static readonly Oid ClientAuth = new("1.3.6.1.5.5.7.3.2");

        private static X509Certificate2 MakeCa(string cn)
        {
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
            return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        }

        private static X509Certificate2 MakeLeaf(string cn, X509Certificate2 issuer)
        {
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { ClientAuth }, true));
            byte[] serial = new byte[8];
            RandomNumberGenerator.Fill(serial);
            // Public part only: the chain check never needs the leaf's private key.
            return req.Create(issuer, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(10), serial);
        }

        private static X509Chain BuildLikeTheFactory(X509Certificate2 leaf, params X509Certificate2[] extra)
        {
            var chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            foreach (var c in extra)
                chain.ChainPolicy.ExtraStore.Add(c);
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            Assert.That(chain.Build(leaf), Is.True, "precondition: Build() is permissive with AllowUnknownCertificateAuthority");
            return chain;
        }

        [Test]
        public void LeafSignedByConfiguredCa_IsAnchored()
        {
            using var ca = MakeCa("Test CA");
            using var leaf = MakeLeaf("good client", ca);
            var chain = BuildLikeTheFactory(leaf, ca);

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, ca, out string problem), Is.True, problem);
            Assert.That(problem, Is.Empty);
        }

        [Test]
        public void LeafFromUnknownIssuer_IsRejected_EvenThoughBuildSucceeded()
        {
            using var ca = MakeCa("Test CA");
            using var rogueCa = MakeCa("Rogue CA");
            using var rogue = MakeLeaf("rogue client", rogueCa);
            var chain = BuildLikeTheFactory(rogue, ca);   // rogue CA is nowhere: partial chain

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, ca, out string problem), Is.False);
            Assert.That(problem, Does.Contain("partial chain"));
        }

        [Test]
        public void LeafFromOtherFullChain_IsRejected()
        {
            using var ca = MakeCa("Test CA");
            using var rogueCa = MakeCa("Rogue CA");
            using var rogue = MakeLeaf("rogue client", rogueCa);
            var chain = BuildLikeTheFactory(rogue, ca, rogueCa);   // chain completes, but to the wrong root

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, ca, out string problem), Is.False);
            Assert.That(problem, Does.Contain("Rogue CA"));
        }

        [Test]
        public void SelfSignedLeaf_PinnedAsItsOwnAnchor_IsAccepted()
        {
            // The counterparty uses a self-signed client cert and the operator configures THAT cert
            // as SSLCACertificate (the Quantifeed/QFEED2 shape, 2025).
            using var selfSigned = MakeSelfSignedClient("mTls-Counterparty");
            var chain = BuildLikeTheFactory(selfSigned, selfSigned);

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, selfSigned, out string problem), Is.True, problem);
        }

        [Test]
        public void SelfSignedLeaf_AgainstAnUnrelatedAnchor_IsRejected()
        {
            // Same self-signed client cert, but SSLCACertificate points at some other root.
            // Before the fix this was accepted: Build() succeeds for a self-signed leaf under
            // AllowUnknownCertificateAuthority and nothing compared it to the configured anchor.
            using var selfSigned = MakeSelfSignedClient("mTls-Counterparty");
            using var unrelatedRoot = MakeCa("TestRootCertificate");
            var chain = BuildLikeTheFactory(selfSigned, unrelatedRoot);

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, unrelatedRoot, out string problem), Is.False);
            Assert.That(problem, Does.Contain("partial chain"));
        }

        private static X509Certificate2 MakeSelfSignedClient(string cn)
        {
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { ClientAuth }, true));
            return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(10));
        }

        [Test]
        public void ConfiguredIntermediate_IsAnchored_WhenChainPassesThroughIt()
        {
            using var root = MakeCa("Root CA");
            using var intermediate = MakeIntermediate("Issuing CA", root);
            using var leaf = MakeLeaf("client", intermediate);
            var chain = BuildLikeTheFactory(leaf, intermediate, root);

            Assert.That(SslStreamFactory.ChainIsAnchoredBy(chain, intermediate, out string problem), Is.True, problem);
        }

        private static X509Certificate2 MakeIntermediate(string cn, X509Certificate2 root)
        {
            using RSA key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
            req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
            byte[] serial = new byte[8];
            RandomNumberGenerator.Fill(serial);
            X509Certificate2 pub = req.Create(root, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(20), serial);
            // The intermediate must sign the leaf, so re-attach its private key.
            return pub.CopyWithPrivateKey(key);
        }
    }
}
