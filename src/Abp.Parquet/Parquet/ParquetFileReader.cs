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
    private readonly ParquetReader _reader;
    private bool _isDisposed = false;

    /// <summary>
    /// 私有构造函数，用于异步初始化
    /// </summary>
    private ParquetFileReader(string filePath, Stream fileStream, ParquetReader reader)
    {
        _filePath = filePath;
        _fileStream = fileStream;
        _reader = reader;
    }

    /// <summary>
    /// 创建Parquet文件读取器的异步工厂方法
    /// </summary>
    /// <param name="filePath">Parquet 文件路径</param>
    /// <returns>ParquetFileReader实例</returns>
    public static async Task<ParquetFileReader> CreateAsync(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException("找不到指定的 Parquet 文件。", filePath);

        Stream fileStream = null;
        ParquetReader reader = null;

        try
        {
            // 打开文件流
            fileStream = File.OpenRead(filePath);
            
            // 创建 Parquet 读取器 - 使用真正的异步调用
            reader = await ParquetReader.CreateAsync(fileStream);
            
            return new ParquetFileReader(filePath, fileStream, reader);
        }
        catch (Exception ex)
        {
            // 确保资源被释放
            reader?.Dispose();
            fileStream?.Dispose();
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
    /// 获取数据集行数 (异步版本)
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录数量</returns>
    public Task<long> GetRowCountAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(() => GetRowCount(), cancellationToken);
    }

    /// <summary>
    /// 读取所有记录 (同步版本)
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
                    // 读取所有列 - 注意这里同步等待异步操作
                    var columns = ReadColumnsAsync(groupReader, fields).GetAwaiter().GetResult();

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
                                    catch (Exception ex)
                                    {
                                        // 记录转换失败的详细信息
                                        System.Diagnostics.Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
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
    /// 读取所有记录 (异步版本)
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <returns>记录集合</returns>
    public async Task<List<T>> ReadRecordsAsync<T>() where T : class, new()
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
                    // 读取所有列 - 直接使用异步方法
                    var columns = await ReadColumnsAsync(groupReader, fields);

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
                                    catch (Exception ex)
                                    {
                                        // 记录转换失败的详细信息
                                        System.Diagnostics.Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
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
    /// 读取所有记录作为动态对象 (同步版本)
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
                    // 读取所有列 - 注意这里同步等待异步操作
                    var columns = ReadColumnsAsync(groupReader, fields).GetAwaiter().GetResult();

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
    /// 读取所有记录作为动态对象 (异步版本)
    /// </summary>
    /// <returns>动态对象集合</returns>
    public async Task<List<Dictionary<string, object>>> ReadRecordsAsync()
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
                    // 读取所有列 - 直接使用异步方法
                    var columns = await ReadColumnsAsync(groupReader, fields);

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
    /// 流式读取记录，适合处理大文件
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <returns>记录流</returns>
    public IEnumerable<T> ReadRecordsStreaming<T>() where T : class, new()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        // 获取类型的属性信息
        var properties = typeof(T).GetProperties();
        
        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列 - 同步等待异步操作
                var columns = ReadColumnsAsync(groupReader, fields).GetAwaiter().GetResult();

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
                                catch (Exception ex)
                                {
                                    // 记录转换失败的详细信息
                                    System.Diagnostics.Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
                                }
                            }
                        }
                    }

                    yield return record;
                }
            }
        }
    }

    /// <summary>
    /// 流式读取记录作为动态对象，适合处理大文件
    /// </summary>
    /// <returns>动态对象流</returns>
    public IEnumerable<Dictionary<string, object>> ReadRecordsStreaming()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列 - 同步等待异步操作
                var columns = ReadColumnsAsync(groupReader, fields).GetAwaiter().GetResult();

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

                    yield return record;
                }
            }
        }
    }

    /// <summary>
    /// 读取指定列的数据
    /// </summary>
    /// <param name="columnName">列名</param>
    /// <returns>列数据</returns>
    public Array ReadColumn(string columnName)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        if (string.IsNullOrWhiteSpace(columnName))
            throw new ArgumentException("列名不能为空", nameof(columnName));

        try
        {
            // 查找对应的字段
            var field = _reader.Schema.Fields
                .OfType<DataField>()
                .FirstOrDefault(f => f.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                
            if (field == null)
                throw new ParquetParserException($"找不到列: {columnName}");

            // 创建适当大小的数组以保存所有行组的数据
            long totalRowCount = GetRowCount();
            var columnType = field.ClrType;
            Array result = Array.CreateInstance(columnType, totalRowCount);

            long currentIndex = 0;
            
            // 读取每个行组的数据
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取当前行组的列 - 同步等待异步操作
                    var dataColumn = groupReader.ReadColumnAsync(field).GetAwaiter().GetResult();
                    
                    // 复制到结果数组
                    if (dataColumn.Data != null)
                    {
                        Array.Copy(dataColumn.Data, 0, result, currentIndex, dataColumn.Data.Length);
                        currentIndex += dataColumn.Data.Length;
                    }
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"读取列 {columnName} 失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 读取指定列的数据 (异步版本)
    /// </summary>
    /// <param name="columnName">列名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>列数据</returns>
    public async Task<Array> ReadColumnAsync(string columnName, CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        if (string.IsNullOrWhiteSpace(columnName))
            throw new ArgumentException("列名不能为空", nameof(columnName));

        try
        {
            // 查找对应的字段
            var field = _reader.Schema.Fields
                .OfType<DataField>()
                .FirstOrDefault(f => f.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                
            if (field == null)
                throw new ParquetParserException($"找不到列: {columnName}");

            // 创建适当大小的数组以保存所有行组的数据
            long totalRowCount = GetRowCount();
            var columnType = field.ClrType;
            Array result = Array.CreateInstance(columnType, totalRowCount);

            long currentIndex = 0;
            
            // 读取每个行组的数据
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取当前行组的列 - 直接使用异步方法
                    var dataColumn = await groupReader.ReadColumnAsync(field);
                    
                    // 复制到结果数组
                    if (dataColumn.Data != null)
                    {
                        Array.Copy(dataColumn.Data, 0, result, currentIndex, dataColumn.Data.Length);
                        currentIndex += dataColumn.Data.Length;
                    }
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"读取列 {columnName} 失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 读取指定行组的所有列
    /// </summary>
    private async Task<Dictionary<string, DataColumn>> ReadColumnsAsync(ParquetRowGroupReader groupReader, DataField[] fields)
    {
        var columns = new Dictionary<string, DataColumn>();
        foreach (var field in fields)
        {
            try
            {
                // 使用异步方法读取列
                var column = await groupReader.ReadColumnAsync(field);
                columns[field.Name] = column;
            }
            catch (Exception ex)
            {
                // 如果读取某列失败，记录异常并继续
                System.Diagnostics.Debug.WriteLine($"读取列 {field.Name} 失败: {ex.Message}");
            }
        }
        return columns;
    }

    /// <summary>
    /// 获取所有列名
    /// </summary>
    /// <returns>列名集合</returns>
    public string[] GetColumnNames()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        return _reader.Schema.Fields
            .OfType<DataField>()
            .Select(f => f.Name)
            .ToArray();
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
    /// 验证文件是否为有效的 Parquet 格式 (异步版本)
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>如果是有效格式则返回 true，否则返回 false</returns>
    public Task<bool> ValidateFormatAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(() => ValidateFormat(), cancellationToken);
    }

    /// <summary>
    /// 实现 IEnumerable 接口，允许使用 foreach 遍历记录
    /// </summary>
    public IEnumerator GetEnumerator()
    {
        return ReadRecords().GetEnumerator();
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
