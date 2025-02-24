using System.Reflection;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Test")]
namespace Core.Data;

internal interface IIndexer
{
    bool Put(byte[] key, LogRecordPos pos);
    LogRecordPos? Get(byte[] key);
    bool Delete(byte[] key);

}