using System;
using System.Diagnostics;

namespace Raven.Client.Util;

internal static class ClusterWideTransactionHelper
{
    public static bool IsAtomicGuardKey(string id, out string docId)
    {
        if (IsAtomicGuardKey(id) == false)
        {
            docId = null;
            return false;
        }

        docId = ExtractDocumentIdFromAtomicGuard(id);
        return true;
    }
    
    public static bool IsAtomicGuardKey(string key) => key.StartsWith(Constants.CompareExchange.RvnAtomicPrefix, StringComparison.OrdinalIgnoreCase);

    public static string GetAtomicGuardKey(string docId) => Constants.CompareExchange.RvnAtomicPrefix + docId;
    public static string GetAtomicGuardKey(ReadOnlyMemory<char> docId) => Constants.CompareExchange.RvnAtomicPrefix + docId;

    public static string ExtractDocumentIdFromAtomicGuard(string key)
    {
        Debug.Assert(key.StartsWith(Constants.CompareExchange.RvnAtomicPrefix));
        return key.Substring(Constants.CompareExchange.RvnAtomicPrefix.Length);
    }
}
