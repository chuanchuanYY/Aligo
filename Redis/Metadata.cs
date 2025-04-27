namespace Redis;

public class Metadata
{
    public Metadata()
    {
        
    }
    
    public Metadata(Types type, long expire, long version, int size, long head, long tail)
    {
        Type = type;
        Expire = expire;
        Version = version;
        Size = size;
        Head = head;
        Tail = tail;
    }

    public Types Type { get; set; }
    public long Expire { get; set; }
    public long Version { get; set; }
    public int Size { get; set; }
    public long Head { get; set; } = 0;
    public long Tail { get; set; } = 0;




    public  byte[] Encode()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write((int)Type);
        writer.Write(Expire);
        writer.Write(Version);
        writer.Write(Size);
        writer.Write(Head);
        writer.Write(Tail);
        return stream.ToArray();
    }

    public static Metadata Decode(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        return new Metadata()
        {
            Type = TypesHelper.GetType(reader.ReadInt32()),
            Expire = reader.ReadInt64(),
            Version = reader.ReadInt64(),
            Size = reader.ReadInt32(),
            Head = reader.ReadInt64(),
            Tail = reader.ReadInt64(),
        };
    }
}