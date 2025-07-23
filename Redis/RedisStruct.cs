using System.Collections.Concurrent;
using System.Security;
using System.Text;
using Core.DB;
using Core.Exceptions;

namespace Redis;

public class RedisStruct: IDisposable
{
    private Engine _engine;

    public RedisStruct(EngineOptions options)
    {
        _engine = new Engine(options);
    }

    public string? Set(string key,string value,TimeSpan? expiry = null )
    {
        // key => value( type|expiry|payload )
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        if (expiry == null)
        {
            expiry = TimeSpan.Zero;
        }
        else
        {
            var now = DateTime.Now;
            expiry = TimeSpan.FromTicks(now.Ticks + expiry.Value.Ticks);
        }
      
        using MemoryStream stream = new MemoryStream();
        using BinaryWriter writer = new BinaryWriter(stream);
        writer.Write((int)Types.String);
        writer.Write(expiry.Value.Ticks);
        writer.Write(value);

        try
        {
            var buf = stream.ToArray();
            var v =  _engine.Put(Encoding.UTF8.GetBytes(key), buf).Value;
            return value;
        }
        catch (Exception e)
        {
            // log ...
            return null;
        }
    }
    
    public string? LPop(string key)
    {
        return InnerPop(key);
    }

    public string? RPop(string key)
    {
        return InnerPop(key,false);
    }
    private string? InnerPop(string key,bool isLeft = true)
    {
        var metadata = FindMetadata(key, Types.List);
        if (metadata.Size == 0)
        {
            return null;
        }
        long index = 0;
            
        if (isLeft)
        {
            index = metadata.Head;
            metadata.Head += 1;
        }
        else
        {
            index = metadata.Tail;
            metadata.Tail -= 1;
        }
        metadata.Size -= 1;
        var internalKey = new ListInternalKey(key,metadata.Version,index);
        try
        {
            var getResult = _engine.Get(internalKey.Encode());

            var writeBatch = _engine.CreateWriteBatch(WriteBatchOptions.Default);
            writeBatch.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
            writeBatch.Delete(internalKey.Encode());
            writeBatch.Commit();
            return Encoding.UTF8.GetString(getResult);
        }
        catch (Exception e)
        {
            return null;
        }
    }
    public bool LPush(string key, string value)
    {
        return InnerPush(key, value);
    }

    public bool RPush(string key, string value)
    {
        return InnerPush(key, value,false);
    }
    private bool InnerPush(string key, string value,bool isLeft = true)
    {
        var metadata = FindMetadata(key, Types.List);
        long index = 0;
        if (isLeft)
        {
            metadata.Head -= 1;
            index = metadata.Head;
        }
        else
        {
            metadata.Tail += 1;
            index = metadata.Tail;
        }
        metadata.Size += 1;
        var internalKey = new ListInternalKey(key, metadata.Version, index);
        try
        {
            var writeBatch = _engine.CreateWriteBatch(WriteBatchOptions.Default);
            writeBatch.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
            writeBatch.Put(internalKey.Encode(),Encoding.UTF8.GetBytes(value));
            writeBatch.Commit();
        }
        catch (Exception e)
        {
            return false;
        }

        return true;
    }
    
    /// <summary>
    /// SREN
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public bool SRemove(string key,string value)
    {
        Metadata metadata = FindMetadata(key,Types.Set);

        if (metadata.Size == 0)
        {
            return false;
        }

        var internalKey = new SetInternalKey(key, metadata.Version, value);
        var writeBatch = _engine.CreateWriteBatch(WriteBatchOptions.Default);
        try
        {
            _engine.Get(internalKey.Encode());
            metadata.Size -= 1;
            writeBatch.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
            writeBatch.Delete(internalKey.Encode());
            writeBatch.Commit();
        }
        catch (KeyNotFoundException e)
        {
            return false;
        }
        
        return true;
    }
    public bool SIsMember(string key,string value)
    {
        Metadata metadata = FindMetadata(key, Types.Set);

        if (metadata.Size == 0)
        {
            return false;
        }
        var internalKey = new SetInternalKey(key,metadata.Version,value);

        try
        {
            _engine.Get(internalKey.Encode());
        }
        catch (KeyNotFoundException _)
        {
            return false;
        }

        return true;
    }
    public bool SAdd(string key, string value)
    {
        Metadata metadata = FindMetadata(key, Types.Set);
        
        // try to find data part 
        var internalKey = new SetInternalKey(key,metadata.Version,value);

        try
        {
            _engine.Get(internalKey.Encode());
        }
        catch (KeyNotFoundException e)
        {
            var writeBatch = _engine.CreateWriteBatch(WriteBatchOptions.Default);

            metadata.Size += 1;

            writeBatch.Put(Encoding.UTF8.GetBytes(key), metadata.Encode());
            writeBatch.Put(internalKey.Encode(), new byte[] { });
            writeBatch.Commit();
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }
    
    public string? Get(string key)
    {
        try
        {
            var v = _engine.Get(Encoding.UTF8.GetBytes(key));
            using MemoryStream stream = new MemoryStream(v);
            using BinaryReader reader = new BinaryReader(stream);
            var type = TypesHelper.GetType(reader.ReadInt32());
            if (type != Types.String)
            {
                return null;
            }
            
            // 判断过期时间
            var now = DateTime.Now;
            var expiry = reader.ReadInt64();
            if (expiry != 0 && now.Ticks > expiry)
            {
                return null;
            }
            return Encoding.UTF8.GetString(reader.ReadBytes(v.Length - sizeof(int) + sizeof(long)));
        }
        catch (Exception e)
        {
            // log ...
            return null;
        }
        
    }
    
    
    /// <summary>
    /// 删除成功返回被删除的数据，没有找到指定的数据或删除失败返回null
    /// </summary>
    /// <param name="key"></param>
    /// <param name="field"></param>
    /// <returns></returns>
    public string? HashDel(string key,string field)
    {
        var metadata = FindMetadata(key, Types.Hash);
        // 当前key 没有字段（field）
        if (metadata.Size == 0)
        {
            return null;
        }
        
        var hashKey = new HashInternalKey(key,metadata.Version,field);
        try
        {
            var value = _engine.Get(hashKey.Encode());

            // if field exist,delete it and then update metadata size 
            var wbOptions = new WriteBatchOptions();
            var wb = _engine.CreateWriteBatch(wbOptions);
            metadata.Size--;
            wb.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
            wb.Delete(hashKey.Encode());
            wb.Commit();
            
            return Encoding.UTF8.GetString(value);
        }
        catch (Exception e)
        {
            return null;
        }
        
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="field"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public string? HashGet(string key, string field)
    {
        var metadata = FindMetadata(key, Types.Hash);
        
        // 当前key 没有字段（field）
        if (metadata.Size == 0)
        {
            return null;
        }
        
        // 根据元素据查找实际的filed 
        var hashKey = new HashInternalKey(key,metadata.Version,field);

        try
        {
            var value = _engine.Get(hashKey.Encode());
            return Encoding.UTF8.GetString(value);
        }
        catch (KeyNotFoundException e)
        {
            return null;
        }
    }
    public bool HashSet(string key,string filed,string value )
    {
        var metadata = FindMetadata(key,Types.Hash);

        var internalKey = new HashInternalKey(key,metadata.Version,filed);
        bool exist = true;
        try
        {
            var result = _engine.Get(internalKey.Encode());
        }
        catch (KeyNotFoundException e)
        {
            exist = false;
        }

        var wbOptions = new WriteBatchOptions();
        var wb = _engine.CreateWriteBatch(wbOptions);
        if (!exist)
        {
            metadata.Size++;
            wb.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
        }
        wb.Put(internalKey.Encode(),Encoding.UTF8.GetBytes(value));
        wb.Commit();
        return true;
    }
    
    
    
    /// <summary>
    /// try to find metadata,if not exists constructor new instance 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    private Metadata FindMetadata(string key,Types type)
    {
        bool exists = true;
        Metadata? metadata = null;
        try
        {
            var result = _engine.Get(Encoding.UTF8.GetBytes(key));
            // decode 
            metadata = Metadata.Decode(result);
            if (metadata.Type != type)
            {
                throw new Exception(ErrorMessage.WrongOperationType);
            }
            
            // 判断过期时间
            var now = DateTime.Now;
            if (metadata.Expire != 0 && now.Ticks > metadata.Expire )
            {
                exists = false;
            }
        }
        catch (KeyNotFoundException e)
        {
            exists = false;
        }

        if (!exists)
        {
            metadata =  new Metadata()
            {
                Type = type,
                Expire = 0,
                Version = DateTime.Now.Ticks,
                Head = 0,
                Tail =  0,
            } ;

            if (type == Types.List)
            {
                metadata.Head = long.MaxValue / 2;
                metadata.Tail = long.MaxValue / 2;
            }
        }
        return metadata!;
    }
    
    #region Generic
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <exception cref="KeyNotFoundException"></exception>
    public void Del(string key)
    {
        _engine.Delete(Encoding.UTF8.GetBytes(key));
        
    }
  
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="KeyNotFoundException"></exception>
    /// <exception cref="Exception"></exception>
    public Types GetType(string key)
    {
        var v = _engine.Get(Encoding.UTF8.GetBytes(key));
        using MemoryStream stream = new MemoryStream(v);
        using BinaryReader reader = new BinaryReader(stream);
        return  TypesHelper.GetType(reader.ReadInt32());
    }
    #endregion


    public bool ZAdd(string key, double score, string member)
    {
        var metadata = FindMetadata(key, Types.ZSet);

        var internalKey = new ZSetInternalKey(key, metadata.Version, member);
        var writeBatch = _engine.CreateWriteBatch(WriteBatchOptions.Default);
        bool isExist = true;
        try
        {
            var oldScore = _engine.Get(internalKey.Encode());
        }
        catch (KeyNotFoundException e)
        {
            isExist = false;
        }
        catch (Exception)
        {
            return false;
        }
        
        if (!isExist)
        {
            metadata.Size += 1;
            writeBatch.Put(Encoding.UTF8.GetBytes(key),metadata.Encode());
        }
        writeBatch.Put(internalKey.Encode(),BitConverter.GetBytes(score));
        writeBatch.Commit();

        return true;
    }

    /// <summary>
    ///  if member not exists,throw Exception 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="member"></param>
    /// <returns></returns>
    public double ZScore(string key, string member)
    {
        var metadata = FindMetadata(key, Types.ZSet);

        if (metadata.Size == 0)
        {
            throw new Exception("member not exist");
        }
        var internalKey = new ZSetInternalKey(key, metadata.Version, member);
        try
        {
            var score = _engine.Get(internalKey.Encode());
            return BitConverter.ToDouble(score);
        }
        catch (Exception e)
        {
            throw new Exception("Internal Exception",e);
        }
        
    }
    public void Dispose()
    {
        _engine.Dispose();
    }
}