using System.Reflection;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Test")]
namespace Core.Data;

internal interface IIndexer: IEnumerable<KeyValuePair<byte[],LogRecordPos>>
{
    bool Put(byte[] key, LogRecordPos pos);
    LogRecordPos? Get(byte[] key);
    bool Delete(byte[] key);
    
    IEnumerable<byte[]> GetKeys();
    
    bool Contains(byte[] key);
   

}