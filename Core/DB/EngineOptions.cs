using Core.IO;

namespace Core.DB;

public class EngineOptions
{
    public string DirPath { get; set; }
    public UInt64 DataFileSize { get; set; }
    public bool AlwaySync { get; set; }

    internal const UInt64 DeafultDataFileSize = 1024 * 1024 * 100; // 100Mb
    
    public IOManagerType IOType { get; set; }
    
    public float ReclaimableRatio { get; set; } // 可回收空间在数据目录的占比，只有在占比大于等于该值的时候才允许Merge

    public EngineOptions(string dirPath, UInt64 dataFileSize = DeafultDataFileSize, bool alwaySync = false,
        IOManagerType ioType = IOManagerType.FileIO, float reclaimableRatio = 0.5f)

    {
        DirPath = dirPath;
        DataFileSize = dataFileSize;
        AlwaySync = alwaySync;
        this.IOType = ioType;
        this.ReclaimableRatio = reclaimableRatio;
    }


}