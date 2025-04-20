using System.Collections;
using System.Text;
using Core.DB;
using Core.IO;
using Test.Common;

namespace Test;

public class DataFileTest
{

     [Test]
     public void TestWrite()
     {
         var path = Path.GetTempFileName();
         IIOManager io = new FileIO(path);
         var datafile = new DataFile(1, io);

         var data = KeyValueHelper.GetValue(1);
         var writeResult = datafile.Write(data);
         Assert.AreEqual(data.Length,writeResult);
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
        Assert.AreEqual(record.Encode().Length,writeResult);
        var readResult = datafile.ReadLogRecord(0);
        Assert.NotNull(readResult);
        Assert.That(() => StructuralComparisons.StructuralEqualityComparer.Equals(record.Key, readResult.Key)
                          && StructuralComparisons.StructuralEqualityComparer.Equals(record.Value, readResult.Value)
                          && record.RecordType == readResult.RecordType);
            
      io.Dispose();
      File.Delete(path);
     
    }
    
    
    [Test]
    public void TestReadMoreThanOne()
    {
        var path = Path.GetTempFileName();
        if (File.Exists(path))
        {
            File.Delete(path);
        }
        IIOManager io = new FileIO(path);
        var datafile = new DataFile(2, io);
       
        for (int i = 0; i < 10; i++)
        {
            var record = new LogRecord()
            {
                Key = KeyValueHelper.GetKey(i),
                Value = KeyValueHelper.GetValue(i),
                RecordType = LogRecordType.Normal,
            };
            var writeResult = datafile.Write(record.Encode());
            Assert.AreEqual(record.Encode().Length,writeResult);
        }

        ulong offset = 0;
        for (int i = 0; i < 10; i++)
        {
            var readResult = datafile.ReadLogRecord(offset);
            Assert.NotNull(readResult);
            Assert.That(() => StructuralComparisons.StructuralEqualityComparer.Equals(KeyValueHelper.GetKey(i), readResult.Key)
                              && StructuralComparisons.StructuralEqualityComparer.Equals(KeyValueHelper.GetValue(i), readResult.Value)
                              && LogRecordType.Normal == readResult.RecordType);
            offset += (ulong)(LogRecord.MaxHeaderSize() + readResult.Key.Length + readResult.Value.Length + 4);
        }
     
            
        io.Dispose();
        File.Delete(path);
     
    }
}