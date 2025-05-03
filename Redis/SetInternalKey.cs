using System.Text;

namespace Redis;

public class SetInternalKey
{
    public SetInternalKey(string key, long version, string member)
    {
        this.key = key;
        Version = version;
        Member = member;
        MemberSize = Encoding.UTF8.GetByteCount(Member);
    }

    public string key { get; set; }
    public long Version { get; set; }
    public string Member { get; set; }
    public int MemberSize { get; set; }

    public byte[] Encode()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        
        writer.Write(Encoding.UTF8.GetBytes(key));
        writer.Write(Version);
        writer.Write(Encoding.UTF8.GetBytes(Member));
        writer.Write(MemberSize);
        return stream.ToArray();
    }

    public static SetInternalKey Decode(byte[] data)
    {
        Range range = new Range(data.Length - 4, data.Length);
        var memberSize =  BitConverter.ToInt32(data[range]);
       
        range = new Range(range.Start.Value - memberSize,range.End.Value -4);
        var memberData = data[range];
        var member = Encoding.UTF8.GetString(memberData);
        range = new Range(range.Start.Value - sizeof(long),range.Start.Value);
        var version = BitConverter.ToInt64(data[range]);
        range = new Range(0, range.Start);
        var key = Encoding.UTF8.GetString(data[range]);
        return new SetInternalKey(key, version, member);
    }
}