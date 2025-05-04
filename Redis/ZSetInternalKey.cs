using System.Text;

namespace Redis;

public class ZSetInternalKey
{
    public ZSetInternalKey(string key, long version, string member)
    {
        Key = key;
        Version = version;
        Member = member;
    }

    public string Key { get; set; }
    public long Version { get; set; }
    public string Member { get; set; }

    public byte[] Encode()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        
        var memberBytes = Encoding.UTF8.GetBytes(Member);
        writer.Write(Version);
        writer.Write(memberBytes.Length);
        writer.Write(memberBytes);
        writer.Write(Key);
        
        return stream.ToArray();
    }

    public ZSetInternalKey Decode(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);
        
        var version = reader.ReadInt64();
        var memberSize = reader.ReadInt32();
        var memberBytes = reader.ReadBytes(memberSize);
        var key = reader.ReadString();

        return new ZSetInternalKey(key, version, Encoding.UTF8.GetString(memberBytes));
    }
}