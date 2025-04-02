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
/// Parquet 解析异常 - 读取或解析时发生错误
/// </summary>
public class ParquetParserException : ParquetException
{
    /// <summary>
    /// 解析失败的文件路径
    /// </summary>
    public string FilePath { get; }

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
    /// <param name="filePath">解析失败的文件路径</param>
    public ParquetParserException(string message, string filePath) : base(message)
    {
        FilePath = filePath;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetParserException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="filePath">解析失败的文件路径</param>
    /// <param name="innerException">内部异常</param>
    public ParquetParserException(string message, string filePath, Exception innerException) : base(message, innerException)
    {
        FilePath = filePath;
    }
}

/// <summary>
/// Parquet 文件写入异常
/// </summary>
public class ParquetWriterException : ParquetException
{
    /// <summary>
    /// 写入操作的目标文件路径
    /// </summary>
    public string FilePath { get; }

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
    /// <param name="filePath">写入操作的目标文件路径</param>
    public ParquetWriterException(string message, string filePath) : base(message)
    {
        FilePath = filePath;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetWriterException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="filePath">写入操作的目标文件路径</param>
    /// <param name="innerException">内部异常</param>
    public ParquetWriterException(string message, string filePath, Exception innerException) : base(message, innerException)
    {
        FilePath = filePath;
    }
}

/// <summary>
/// Parquet 架构异常 - 处理架构定义或验证时的错误
/// </summary>
public class ParquetSchemaException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetSchemaException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetSchemaException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Parquet 类型转换异常 - 处理数据类型转换过程中的错误
/// </summary>
public class ParquetTypeConversionException : ParquetException
{
    /// <summary>
    /// 源类型
    /// </summary>
    public Type SourceType { get; }

    /// <summary>
    /// 目标类型
    /// </summary>
    public Type TargetType { get; }

    /// <summary>
    /// 字段名称
    /// </summary>
    public string FieldName { get; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetTypeConversionException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="sourceType">源类型</param>
    /// <param name="targetType">目标类型</param>
    /// <param name="fieldName">字段名称</param>
    public ParquetTypeConversionException(string message, Type sourceType, Type targetType, string fieldName)
        : base(message)
    {
        SourceType = sourceType;
        TargetType = targetType;
        FieldName = fieldName;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetTypeConversionException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="sourceType">源类型</param>
    /// <param name="targetType">目标类型</param>
    /// <param name="fieldName">字段名称</param>
    /// <param name="innerException">内部异常</param>
    public ParquetTypeConversionException(string message, Type sourceType, Type targetType, string fieldName, Exception innerException)
        : base(message, innerException)
    {
        SourceType = sourceType;
        TargetType = targetType;
        FieldName = fieldName;
    }
}

/// <summary>
/// Parquet 内存限制异常 - 当操作超出内存限制时使用
/// </summary>
public class ParquetMemoryLimitException : ParquetException
{
    /// <summary>
    /// 请求的内存大小（字节）
    /// </summary>
    public long RequestedBytes { get; }

    /// <summary>
    /// 可用的内存限制（字节）
    /// </summary>
    public long AvailableBytes { get; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="requestedBytes">请求的内存大小</param>
    /// <param name="availableBytes">可用内存限制</param>
    public ParquetMemoryLimitException(string message, long requestedBytes, long availableBytes)
        : base(message)
    {
        RequestedBytes = requestedBytes;
        AvailableBytes = availableBytes;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetMemoryLimitException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Parquet 操作取消异常 - 当操作被用户取消时使用
/// </summary>
public class ParquetOperationCanceledException : ParquetException
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    public ParquetOperationCanceledException(string message) : base(message)
    {
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="message">错误消息</param>
    /// <param name="innerException">内部异常</param>
    public ParquetOperationCanceledException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
