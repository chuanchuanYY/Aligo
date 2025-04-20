using System.Reflection;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Test")]
namespace Core.Data;

internal interface IIndexer: IEnumerable<KeyValuePair<byte[],LogRecordPos>>
{  
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <param name="pos"></param>
    /// <returns>ture if success otherwise false</returns>
    bool Put(byte[] key, LogRecordPos pos);
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns>null if key notfound otherwise LogRecordPos</returns>
    LogRecordPos? Get(byte[] key);
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns>ture if success otherwise flase</returns>
    bool Delete(byte[] key);
    
    /// <summary>
    ///  get all keys 
    /// </summary>
    /// <returns></returns>
    IEnumerable<byte[]> GetKeys();
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="key"></param>
    /// <returns>ture if contains key otherwise false</returns>
    bool Contains(byte[] key);
   

}