namespace Core.Common;

public class DBHelper
{
    public static long GetDirectorySize(string dirPath)
    {
        var files =  new DirectoryInfo(dirPath).GetFiles();
        return files.Sum(f =>
        {
             f.Refresh();
             return f.Length;
        });
    }


    public static void CopyDirectory(string sourceDirName, string destDirName)
    {
        Directory.GetFiles(sourceDirName)
            .ToList()
            .ForEach(f =>
            {
                File.Copy(f, Path.Combine(destDirName, Path.GetFileName(f)));
            });
    }
}