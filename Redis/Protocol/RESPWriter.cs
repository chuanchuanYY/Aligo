using System.Net.Sockets;
using System.Text;

namespace Redis.Protocol;

public class RESPWriter
{
    private Socket _socket;

    public RESPWriter(Socket socket)
    {
        _socket = socket;
    }
    
    /// <summary>
    /// write simple string 
    /// </summary>
    /// <param name="data"></param>
    public  Task<int> WriteAsync(string data)
    {
      return   BuildCommandAndSendAsync('+', data);          
    }

    public  Task<int> ErrorAsync(string data)
    {
        return  BuildCommandAndSendAsync('-', data);
    }
    
    
    
    private  Task<int> BuildCommandAndSendAsync(char type,string data)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append(type);
        sb.Append(data);
        sb.Append("\r\n");  
        
        return  _socket.SendAsync(Encoding.ASCII.GetBytes(sb.ToString()),SocketFlags.None);
    }

 
}