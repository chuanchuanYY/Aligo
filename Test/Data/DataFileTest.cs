using System.Collections;
using System.Text;
using Core.DB;
using Core.IO;

namespace Test;

public class DataFileTest
{

     [Test]
     public void TestWrite()
     {
         var path = Path.GetTempFileName();
         IIOManager io = new FileIO(path);
         var datafile = new DataFile(1, io);
         
         var writeResult = datafile.Write(Encoding.UTF8.GetBytes("Hello World"));
         Assert.IsTrue(writeResult);
         io.Dispose();
         File.Delete(path);
      
     }
    [Test]
    public void TestRead()
    {
        var path = Path.GetTempFileName();
       
        IIOManager io = new FileIO(path);
        var datafile = new DataFile(2, io);
        var record = new LogRecord()
        {
            Key = Encoding.UTF8.GetBytes("key1"),
            Value = Encoding.UTF8.GetBytes("value1"),
            RecordType = LogRecordType.Normal,
        };
        var writeResult = datafile.Write(record.Encode());
        Assert.IsTrue(writeResult);
        var readResult = datafile.ReadLogRecord(0);
        Assert.NotNull(readResult);
        Assert.That(() => StructuralComparisons.StructuralEqualityComparer.Equals(record.Key, readResult.Key)
                          && StructuralComparisons.StructuralEqualityComparer.Equals(record.Value, readResult.Value)
                          && record.RecordType == readResult.RecordType);
            
      io.Dispose();
      File.Delete(path);
     
    }
}