namespace Abp.Parquet;

/// <summary>
/// Parquet 文件写入异常
/// </summary>
public class ParquetWriterException : Exception
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetWriterException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetWriterException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
