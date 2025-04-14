using System.Text;
using Core.IO;

namespace Test;

public class MemoryMapTest
{
    [Test]
    public void TestRead()
    {
        string path = Path.Join(Path.GetTempPath(), Path.GetRandomFileName());
        var data = Encoding.UTF8.GetBytes("Test Write");
        var fileStream = new FileStream(path, FileMode.Create, FileAccess.Write,  FileShare.ReadWrite);
        fileStream.Write(data);
        fileStream.Dispose();
      
        using var mapIO = new MemoryMap(path);
        byte[] readBuf = new byte[data.Length];
        var readReault = mapIO.Read(readBuf,0);
        

        Assert.AreEqual(data.Length,readReault );
        Assert.AreEqual(data,readBuf);
        mapIO.Dispose();
        File.Delete(path);
    }
}