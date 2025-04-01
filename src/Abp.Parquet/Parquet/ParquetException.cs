namespace Abp.Parquet;

/// <summary>
/// Parquet 操作基础异常
/// </summary>
public abstract class ParquetException : Exception
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    protected ParquetException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    protected ParquetException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Parquet 解析异常
/// </summary>
public class ParquetParserException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetParserException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetParserException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Parquet 文件写入异常
/// </summary>
public class ParquetWriterException : ParquetException
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
