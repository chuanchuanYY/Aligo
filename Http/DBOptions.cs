namespace Http;

public class DBOptions
{
    public string DirPath { get; set; }
    public UInt64 DataFileSize { get; set; }
    public bool AlwaySync { get; set; }
    public string IOType { get; set; }
    public float ReclaimableRatio { get; set; }
}