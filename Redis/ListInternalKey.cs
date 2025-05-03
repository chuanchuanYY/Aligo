namespace Redis;

public class ListInternalKey
{
    public ListInternalKey(string key, long version, long index)
    {
        Key = key;
        Version = version;
        Index = index;
    }

    public string Key { get; set; }
    public long Version { get; set; }
    public long Index { get; set; }


    public  byte[] Encode()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(Version);
        writer.Write(Index);
        writer.Write(Key);
        
        return stream.ToArray();
    }

    public static ListInternalKey Decode(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);
        
        var version = reader.ReadInt64();
        var index = reader.ReadInt64();
        var key = reader.ReadString();

        return new ListInternalKey(key,version,index);
    }
}