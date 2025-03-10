using System.Collections;
using System.Text;
using Core.DB;

namespace Test;

public class LogRecordTest
{

    [Test]
    public void TestLogRecordEncodeAndDecode()
    {
        var record = new LogRecord()
        {
            Key = Encoding.UTF8.GetBytes("key1"),
            Value = Encoding.UTF8.GetBytes("value1"),
            RecordType = LogRecordType.Normal
        };
        var encoded = record.Encode();

        var decoded = LogRecord.Decode(encoded);
        
        Assert.That(() => StructuralComparisons.StructuralEqualityComparer.Equals(record.Key, decoded.Key)
                          && StructuralComparisons.StructuralEqualityComparer.Equals(record.Value, decoded.Value)
                          && record.RecordType == decoded.RecordType);
    }
}