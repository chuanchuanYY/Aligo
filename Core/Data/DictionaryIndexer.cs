using System.Collections;
using Microsoft.Extensions.Logging;

namespace Core.Data;
using System.Collections.Concurrent;
internal class DictionaryIndexer: IIndexer
{
    private readonly ConcurrentDictionary<byte[], LogRecordPos> _dictionary 
        = new(new DictionaryByteArrayComparer());

    private ILogger<DictionaryIndexer> _logger;

    public DictionaryIndexer(ILogger<DictionaryIndexer> logger)
    {
        _logger = logger;
    }
    public bool Put(byte[] key, LogRecordPos pos)
    {
        try
        {
            var result = _dictionary.AddOrUpdate(key,
                (k) => pos,
                (k,v)=>pos);
        }
        catch (Exception e)
        {
           // log ...
           _logger.LogWarning(e,$"failed to add or update key {key}");
           return false;
        }
        return true;
    }

    public LogRecordPos? Get(byte[] key)
    {
        return _dictionary.GetValueOrDefault(key);
    }

    public bool Delete(byte[] key)
    {
        return _dictionary.Remove(key, out var value);
    }
}


internal class DictionaryByteArrayComparer : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y)
    {
        
        return StructuralComparisons.StructuralEqualityComparer.Equals(x, y);
    }

    public int GetHashCode(byte[] obj)
    {
        return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
    }
}