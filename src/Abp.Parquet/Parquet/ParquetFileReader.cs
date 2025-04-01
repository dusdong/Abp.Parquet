using System.Collections;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// Parquet 文件读取器
/// </summary>
public class ParquetFileReader : IDisposable, IEnumerable
{
    private readonly string _filePath;
    private readonly Stream _fileStream;
    private ParquetReader _reader;
    private bool _isDisposed = false;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="filePath">Parquet 文件路径</param>
    public ParquetFileReader(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException("找不到指定的 Parquet 文件。", filePath);

        _filePath = filePath;

        try
        {
            // 打开文件流
            _fileStream = File.OpenRead(_filePath);

            // 创建 Parquet 读取器 - 使用异步方法的同步调用
            _reader = Task.Run(async () =>
                await ParquetReader.CreateAsync(_fileStream)
            ).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            // 确保资源被释放
            _fileStream?.Dispose();
            throw new ParquetParserException($"无法打开 Parquet 文件: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 获取数据集行数
    /// </summary>
    /// <returns>记录数量</returns>
    public long GetRowCount()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        try
        {
            // 汇总所有行组的行数
            long totalRows = 0;
            for (int i = 0; i < _reader.RowGroupCount; i++)
            {
                using (var groupReader = _reader.OpenRowGroupReader(i))
                {
                    totalRows += groupReader.RowCount;
                }
            }

            return totalRows;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"无法获取 Parquet 文件行数: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 读取所有记录
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <returns>记录集合</returns>
    public List<T> ReadRecords<T>() where T : class, new()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        var results = new List<T>();

        try
        {
            // 获取类型的属性信息
            var properties = typeof(T).GetProperties();

            // 获取所有字段
            var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

            // 处理每个行组
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取所有列
                    var columns = new Dictionary<string, DataColumn>();
                    foreach (var field in fields)
                    {
                        try
                        {
                            var column = Task.Run(async () =>
                                await groupReader.ReadColumnAsync(field)
                            ).GetAwaiter().GetResult();

                            columns[field.Name] = column;
                        }
                        catch (Exception ex)
                        {
                            // 如果读取某列失败，记录异常并继续
                            System.Diagnostics.Debug.WriteLine($"读取列 {field.Name} 失败: {ex.Message}");
                        }
                    }

                    // 处理每一行数据
                    long rowCount = groupReader.RowCount;
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                    {
                        var record = new T();

                        // 设置每个属性的值
                        foreach (var property in properties)
                        {
                            if (columns.TryGetValue(property.Name, out var column) &&
                                column.Data != null &&
                                rowIndex < column.Data.Length)
                            {
                                object value = column.Data.GetValue(rowIndex);
                                if (value != null)
                                {
                                    try
                                    {
                                        // 转换并设置属性值
                                        property.SetValue(record, Convert.ChangeType(value, property.PropertyType));
                                    }
                                    catch
                                    {
                                        // 如果无法转换，跳过该属性
                                    }
                                }
                            }
                        }

                        results.Add(record);
                    }
                }
            }

            return results;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"读取 Parquet 文件记录失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 读取所有记录作为动态对象
    /// </summary>
    /// <returns>动态对象集合</returns>
    public List<Dictionary<string, object>> ReadRecords()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        var results = new List<Dictionary<string, object>>();

        try
        {
            // 获取所有字段
            var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

            // 处理每个行组
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取所有列
                    var columns = new Dictionary<string, DataColumn>();
                    foreach (var field in fields)
                    {
                        try
                        {
                            var column = Task.Run(async () =>
                                await groupReader.ReadColumnAsync(field)
                            ).GetAwaiter().GetResult();

                            columns[field.Name] = column;
                        }
                        catch (Exception ex)
                        {
                            // 如果读取某列失败，记录异常并继续
                            System.Diagnostics.Debug.WriteLine($"读取列 {field.Name} 失败: {ex.Message}");
                        }
                    }

                    // 处理每一行数据
                    long rowCount = groupReader.RowCount;
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                    {
                        var record = new Dictionary<string, object>();

                        // 设置每个字段的值
                        foreach (var field in fields)
                        {
                            if (columns.TryGetValue(field.Name, out var column) &&
                                column.Data != null &&
                                rowIndex < column.Data.Length)
                            {
                                record[field.Name] = column.Data.GetValue(rowIndex);
                            }
                            else
                            {
                                record[field.Name] = null;
                            }
                        }

                        results.Add(record);
                    }
                }
            }

            return results;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"读取 Parquet 文件记录失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 实现 IEnumerable 接口，允许使用 foreach 遍历记录
    /// </summary>
    public IEnumerator GetEnumerator()
    {
        return ReadRecords().GetEnumerator();
    }

    /// <summary>
    /// 验证文件是否为有效的 Parquet 格式
    /// </summary>
    /// <returns>如果是有效格式则返回 true，否则返回 false</returns>
    public bool ValidateFormat()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        try
        {
            // 尝试读取元数据和第一个行组，如果成功则表明文件格式正确
            if (_reader.RowGroupCount > 0)
            {
                using (var groupReader = _reader.OpenRowGroupReader(0))
                {
                    return true;
                }
            }

            return true; // 文件格式正确，但没有数据
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;

        _reader?.Dispose();
        _fileStream?.Dispose();
        _isDisposed = true;

        GC.SuppressFinalize(this);
    }
}
