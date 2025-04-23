using System.Text;
using Core.DB;

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

    public void Dispose()
    {
        _engine.Dispose();
    }
}