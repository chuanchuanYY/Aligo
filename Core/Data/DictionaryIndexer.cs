using System.Collections;
using CommunityToolkit.Diagnostics;
using Core.Common;
using Microsoft.Extensions.Logging;

namespace Core.Data;
using System.Collections.Concurrent;
internal class DictionaryIndexer: IIndexer
{
    private readonly ConcurrentDictionary<byte[], LogRecordPos> _dictionary 
        = new(new ByteArrayEqualityComparer());

    private ILogger<DictionaryIndexer> _logger;

    public DictionaryIndexer()
    {
        _logger = Log.Factory.CreateLogger<DictionaryIndexer>();
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="pos"></param>
    /// <exception cref="ArgumentNullException">Argument Can not be null</exception>
    /// <returns></returns>
    public bool Put(byte[] key, LogRecordPos pos)
    {
        Guard.IsNotNull(key);
        Guard.IsNotNull(pos);
        try
        {
            var result = _dictionary.AddOrUpdate(key,
                (k) => pos,
                (k,v)=>pos);
        }
        catch (Exception e)
        {
           _logger.LogError(e,$"failed to add or update key {key}");
           return false;
        }
        return true;
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <exception cref="ArgumentNullException">argument can not be null</exception>
    /// <returns></returns>
    public LogRecordPos? Get(byte[] key)
    {
        Guard.IsNotNull(key);
        return _dictionary.GetValueOrDefault(key);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <exception cref="ArgumentNullException">argument can not be null</exception>
    /// <returns></returns>
    public bool Delete(byte[] key)
    {
        Guard.IsNotNull(key);
        return _dictionary.Remove(key, out var value);
    }

    public IEnumerable<byte[]> GetKeys()
    {
        return _dictionary.Keys;
    }

    public bool Contains(byte[] key)
    {
        return _dictionary.ContainsKey(key);
    }


    public IEnumerator<KeyValuePair<byte[], LogRecordPos>> GetEnumerator()
    {
        return _dictionary.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return this.GetEnumerator();
    }
}

