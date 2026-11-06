using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using QuickFix.Util;

namespace QuickFix.Transport;

internal static class SslCertCache {

    /// <summary>
    /// Cache loaded certificates since loading a certificate can be a costly operation
    /// </summary>
    private static readonly Dictionary<string, CacheEntry> CertificateCache = new ();

    /// <summary>
    /// What a certificate file looked like on disk when it was read. Null for a certificate that
    /// came from the certificate store (no file to watch).
    /// </summary>
    private readonly record struct FileStamp(DateTime LastWriteTimeUtc, long Length);

    private sealed class CacheEntry
    {
        internal CacheEntry(X509Certificate2 certificate, FileStamp? stamp)
        {
            Certificate = certificate;
            Stamp = stamp;
        }

        internal X509Certificate2 Certificate { get; }

        /// <summary>The file state <see cref="Certificate"/> was loaded from.</summary>
        internal FileStamp? Stamp { get; }

        /// <summary>The newer file state that failed to load, so the failure is logged once, not per connection.</summary>
        internal FileStamp? FailedStamp { get; set; }
    }

    /// <summary>
    /// Loads the specified certificate given a path, DistinguishedName or subject name.
    /// A certificate loaded from a file is re-read when the file changes on disk (last write time
    /// or length), so a renewed certificate dropped over the old one under the same name is used by
    /// the next connection without restarting the process. Connections that are already established
    /// are not affected. If the changed file cannot be loaded (half-written, wrong password), the
    /// certificate that was already in use keeps being served and the load is retried on the next call.
    /// </summary>
    /// <param name="name">The certificate path or DistinguishedName/subjectname
    /// if it should be loaded from the personal certificate store.</param>
    /// <param name="password">The certificate password.</param>
    /// <param name="log">Optional sink for "certificate changed" / "changed but unusable" events.</param>
    /// <returns>The specified certificate</returns>
    internal static X509Certificate2? LoadCertificate(string name, string? password, Action<string>? log = null)
    {
        lock (CertificateCache)
        {
            FileStamp? stamp = GetFileStamp(name);

            if (CertificateCache.TryGetValue(name, out CacheEntry? entry))
            {
                // Unchanged, or the file is gone for the moment (mid-swap, or a store-based name):
                // keep using what we have.
                if (stamp is null || stamp == entry.Stamp)
                    return entry.Certificate;

                try
                {
                    X509Certificate2 renewed = LoadCertificateInner(name, password);
                    // The previous certificate is deliberately NOT disposed: a handshake that started
                    // before the swap may still be using it. It is released by the finalizer.
                    CertificateCache[name] = new CacheEntry(renewed, stamp);
                    log?.Invoke(
                        $"Certificate '{name}' changed on disk: now using thumbprint {renewed.Thumbprint}, " +
                        $"valid to {renewed.NotAfter:yyyy-MM-dd} (was thumbprint {entry.Certificate.Thumbprint}, " +
                        $"valid to {entry.Certificate.NotAfter:yyyy-MM-dd})");
                    return renewed;
                }
                catch (Exception ex)
                {
                    if (entry.FailedStamp != stamp)
                    {
                        entry.FailedStamp = stamp;
                        log?.Invoke(
                            $"Certificate '{name}' changed on disk but could not be loaded ({ex.Message.Trim()}); " +
                            $"still using thumbprint {entry.Certificate.Thumbprint}, " +
                            $"valid to {entry.Certificate.NotAfter:yyyy-MM-dd}");
                    }
                    return entry.Certificate;
                }
            }

            try {
                X509Certificate2 certificate = LoadCertificateInner(name, password);
                CertificateCache.Add(name, new CacheEntry(certificate, stamp));

                return certificate;
            } catch (ApplicationException) {
                // TODO refactor this function+callers to throw an exception up the stack instead of returning null
                // Callers should log as appropriate
                return null;
            }
        }
    }

    /// <summary>
    /// The on-disk state of a certificate file, or null when <paramref name="name"/> is not an
    /// existing file (a store lookup name, or a file that is missing right now).
    /// </summary>
    private static FileStamp? GetFileStamp(string name)
    {
        try
        {
            var info = new FileInfo(StringUtil.FixSlashes(name));
            return info.Exists ? new FileStamp(info.LastWriteTimeUtc, info.Length) : null;
        }
        catch (Exception)
        {
            // An unreadable or malformed path is treated like "no file": nothing to compare against.
            return null;
        }
    }

    /// <summary>
    /// Perform the actual loading of a certificate
    /// </summary>
    /// <param name="name">The certificate path or DistinguishedName/subjectname if it should be loaded from the personal certificate store.</param>
    /// <param name="password">The certificate password.</param>
    /// <exception cref="ApplicationException">Certificate could not be loaded from file or store</exception>
    /// <returns>The specified certificate</returns>
    private static X509Certificate2 LoadCertificateInner(string name, string? password)
    {
        var certPath = StringUtil.FixSlashes(name);

        // If cert file not found, then try to get from certificate store
        if (!File.Exists(certPath))
        {
            var certFromStore = GetCertificateFromStore(StringUtil.FixSlashes(name));
            if (certFromStore is not null)
                return certFromStore;

            string msg =
                $"Certificate '{name}' could not be loaded from store or path '{Directory.GetCurrentDirectory()}'";
            Console.WriteLine(msg);
            throw new ApplicationException(msg);
        }

        return password is not null
                ? new X509Certificate2(name, password)
                : new X509Certificate2(name);
    }

    /// <summary>
    /// Gets the certificate from store.
    /// </summary>
    /// <remarks>See http://msdn.microsoft.com/en-us/library/system.security.cryptography.x509certificates.x509certificate2.aspx for complete example</remarks>
    /// <param name="certName">Name of the cert.</param>
    /// <returns>The cert, or null if not found</returns>
    private static X509Certificate2? GetCertificateFromStore(string certName)
    {
        return GetCertificateFromStoreHelper(certName, new X509Store(StoreLocation.LocalMachine))
            ?? GetCertificateFromStoreHelper(certName, new X509Store(StoreLocation.CurrentUser));
    }

    private static X509Certificate2? GetCertificateFromStoreHelper(string certName, X509Store store)
    {
        try
        {
            store.Open(OpenFlags.ReadOnly);

            // Place all certificates in an X509Certificate2Collection object.
            X509Certificate2Collection certCollection = store.Certificates;
            // If using a certificate with a trusted root you do not need to FindByTimeValid, instead:
            // currentCerts.Find(X509FindType.FindBySubjectDistinguishedName, certName, true);
            X509Certificate2Collection currentCerts = certCollection.Find(X509FindType.FindByTimeValid, DateTime.Now, false);

            currentCerts = currentCerts.Find(certName.Contains("CN=")
                ? X509FindType.FindBySubjectDistinguishedName
                : X509FindType.FindBySubjectName, certName, false);

            if (currentCerts.Count == 0)
                return null;

            return currentCerts[0];
        }
        finally
        {
            store.Close();
        }
    }
}
