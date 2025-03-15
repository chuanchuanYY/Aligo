namespace Core.DB;

public class EngineOptions
{
    public string DirPath { get; set; }
    public UInt64 DataFileSize { get; set; }
    public bool AlwaySync { get; set; }

    public const UInt64 DeafultDataFileSize = 1024 * 1024 * 100; // 100Mb
     public EngineOptions(string dirPath,UInt64 dataFileSize = DeafultDataFileSize, bool alwaySync = false)
    {
        DirPath = dirPath;
        DataFileSize = dataFileSize;
        AlwaySync = alwaySync;
    }

    
}