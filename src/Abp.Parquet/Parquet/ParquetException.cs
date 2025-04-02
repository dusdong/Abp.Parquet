namespace Abp.Parquet;

/// <summary>
/// Parquet 解析异常 - 读取或解析时发生错误
/// </summary>
public class ParquetParserException : Exception
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
public class ParquetWriterException : Exception
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
/// Parquet 类型转换异常 - 处理数据类型转换过程中的错误
/// </summary>
public class ParquetTypeConversionException : Exception
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
    public ParquetTypeConversionException(string message, Type sourceType, Type targetType, string fieldName) : base(message)
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
    public ParquetTypeConversionException(string message, Type sourceType, Type targetType, string fieldName, Exception innerException) : base(message, innerException)
    {
        SourceType = sourceType;
        TargetType = targetType;
        FieldName = fieldName;
    }
}

/// <summary>
/// Parquet 操作取消异常 - 当操作被用户取消时使用
/// </summary>
public class ParquetOperationCanceledException : Exception
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
