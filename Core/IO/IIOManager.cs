namespace Core.IO;

internal interface IIOManager: IDisposable
{   
    
    /// <summary>
    /// 从指定偏移量offset读取数据到buf，读取的长度为buf的长度。
    /// </summary>
    /// <param name="buf">存储数据的缓冲区</param>
    /// <param name="offset">读取偏移量</param>
    /// <exception cref="IOException"></exception>
    /// <exception cref="ArgumentNullException"></exception>
    /// <returns>读取的长度</returns>
    UInt64 Read(byte[] buf, UInt64 offset);
    
    /// <summary>
    /// 将buf以追加的方式写入
    /// </summary>
    /// <param name="buf">要写入的数据</param>
    /// <exception cref="IOException"></exception>
    /// <exception cref="ArgumentNullException"></exception>
    /// <returns>写入的长度</returns>
    UInt64 Write(byte[] buf);
    
    /// <summary>
    /// 将数据从缓冲区写入磁盘，以磁盘IO为例
    /// </summary>
    /// <exception cref="IOException"></exception>
    void Sync();
}