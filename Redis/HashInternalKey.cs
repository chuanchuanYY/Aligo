using System.Text;

namespace Redis;

public class HashInternalKey
{
    public HashInternalKey(string key, long version, string field)
    {
        Key = key;
        Version = version;
        Field = field;
    }

    public string Key { get; set; }
    public  long Version { get; set; }
    public string Field { get; set; }


    public byte[] Encode()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(Encoding.UTF8.GetBytes(Key));
        writer.Write(Version);
        writer.Write(Encoding.UTF8.GetBytes(Field));
        return stream.ToArray();
    }
}