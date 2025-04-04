using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// Parquet 文件读取器
/// </summary>
public class ParquetFileReader : IDisposable, IAsyncDisposable, IEnumerable<Dictionary<string, object>>
{
    private readonly string _filePath;
    private readonly Stream _fileStream;
    private readonly ParquetReader _reader;
    private bool _isDisposed = false;
    private long? _cachedRowCount = null;

    // 缓冲区池
    private static readonly BufferPool _bufferPool = new BufferPool();

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
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>ParquetFileReader实例</returns>
    public static async Task<ParquetFileReader> CreateAsync(string filePath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException("找不到指定的 Parquet 文件。", filePath);

        Stream fileStream = null;
        ParquetReader reader = null;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 打开文件流
            fileStream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                4096,
                FileOptions.Asynchronous);

            // 创建 Parquet 读取器 - 使用真正的异步调用
            reader = await ParquetReader.CreateAsync(fileStream).ConfigureAwait(false);

            return new ParquetFileReader(filePath, fileStream, reader);
        }
        catch (OperationCanceledException)
        {
            // 安全释放资源
            ResourceManager.SafeDispose(reader);
            await ResourceManager.SafeCloseStreamAsync(fileStream);

            throw new ParquetOperationCanceledException("Parquet 文件打开操作已取消。", "Open", filePath);
        }
        catch (Exception ex)
        {
            // 安全释放资源
            ResourceManager.SafeDispose(reader);
            await ResourceManager.SafeCloseStreamAsync(fileStream);

            var context = new Dictionary<string, object>
            {
                { "FileSize", File.Exists(filePath) ? new FileInfo(filePath).Length.ToString() : "Unknown" }
            };

            if (ex is IOException ioEx)
            {
                throw new ParquetParserException($"打开 Parquet 文件时发生 I/O 错误: {ioEx.Message}", 
                    "Open", filePath, context, ioEx);
            }

            throw new ParquetParserException($"无法打开 Parquet 文件: {ex.Message}",
                "Open", filePath, context, ex);
        }
    }

    /// <summary>
    /// 获取数据集行数
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录数量</returns>
    public long GetRowCount(CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        if (_cachedRowCount.HasValue)
            return _cachedRowCount.Value;

        try
        {
            // 汇总所有行组的行数
            long totalRows = 0;
            for (int i = 0; i < _reader.RowGroupCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var groupReader = _reader.OpenRowGroupReader(i))
                {
                    totalRows += groupReader.RowCount;
                }
            }

            _cachedRowCount = totalRows;
            return totalRows;
        }
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("获取行数操作已取消。", "GetRowCount", _filePath);
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"无法获取 Parquet 文件行数: {ex.Message}", 
                "GetRowCount", _filePath, null, ex);
        }
    }

    /// <summary>
    /// 获取数据集行数 (异步版本)
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录数量</returns>
    public Task<long> GetRowCountAsync(CancellationToken cancellationToken = default)
    {
        // 确保避免死锁
        return Task.Run(() => GetRowCount(cancellationToken), cancellationToken);
    }

    /// <summary>
    /// 读取所有记录 (同步版本)
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录集合</returns>
    public List<T> ReadRecords<T>(CancellationToken cancellationToken = default) where T : class, new()
    {
        // 确保避免死锁
        return Task.Run(() => ReadRecordsAsync<T>(cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(), 
            cancellationToken).GetAwaiter().GetResult();
    }

    /// <summary>
    /// 读取所有记录 (异步版本)
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录集合</returns>
    public async Task<List<T>> ReadRecordsAsync<T>(CancellationToken cancellationToken = default) where T : class, new()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        var results = new List<T>();
        var propertyAccessor = new PropertyAccessor<T>(typeof(T).GetProperties());

        try
        {
            // 获取所有字段
            var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

            // 处理每个行组
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取所有列
                    var columns = await ReadColumnsAsync(groupReader, fields, cancellationToken);

                    // 处理每一行数据
                    long rowCount = groupReader.RowCount;
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var record = new T();

                        // 设置每个属性的值
                        foreach (var property in typeof(T).GetProperties())
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
                                        // 使用预编译的属性设置器
                                        if (propertyAccessor.HasProperty(property.Name))
                                        {
                                            var convertedValue = Convert.ChangeType(value, property.PropertyType);
                                            propertyAccessor.SetValue(record, property.Name, convertedValue);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        // 记录转换失败的详细信息
                                        Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
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
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("读取记录操作已取消。", "ReadRecords", _filePath);
        }
        catch (ParquetException)
        {
            // 直接重新抛出 Parquet 异常
            throw;
        }
        catch (Exception ex)
        {
            var context = new Dictionary<string, object>
            {
                { "RecordType", typeof(T).Name },
                { "RowGroupCount", _reader.RowGroupCount }
            };

            throw new ParquetParserException($"异步读取 Parquet 文件记录失败: {ex.Message}", 
                "ReadRecords", _filePath, context, ex);
        }
    }

    /// <summary>
    /// 读取所有记录作为动态对象 (同步版本)
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>动态对象集合</returns>
    public List<Dictionary<string, object>> ReadRecords(CancellationToken cancellationToken = default)
    {
        // 确保避免死锁
        return Task.Run(() => ReadRecordsAsync(cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(), 
            cancellationToken).GetAwaiter().GetResult();
    }

    /// <summary>
    /// 读取所有记录作为动态对象 (异步版本)
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>动态对象集合</returns>
    public async Task<List<Dictionary<string, object>>> ReadRecordsAsync(CancellationToken cancellationToken = default)
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
                cancellationToken.ThrowIfCancellationRequested();

                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取所有列
                    var columns = await ReadColumnsAsync(groupReader, fields, cancellationToken);

                    // 处理每一行数据
                    long rowCount = groupReader.RowCount;
                    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

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
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("读取记录操作已取消。", "ReadRecords", _filePath);
        }
        catch (ParquetException)
        {
            // 直接重新抛出 Parquet 异常
            throw;
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"异步读取 Parquet 文件记录失败: {ex.Message}", 
                "ReadRecords", _filePath, null, ex);
        }
    }

    /// <summary>
    /// 异步流式读取记录，适合处理大文件
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录流</returns>
    public async IAsyncEnumerable<T> ReadRecordsStreamingAsync<T>(
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class, new()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        var propertyAccessor = new PropertyAccessor<T>(typeof(T).GetProperties());

        // 获取类型的属性信息
        var properties = typeof(T).GetProperties();

        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列
                var columns = await ReadColumnsAsync(groupReader, fields, cancellationToken);

                // 处理每一行数据
                long rowCount = groupReader.RowCount;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
                                    // 使用预编译的属性设置器
                                    if (propertyAccessor.HasProperty(property.Name))
                                    {
                                        var convertedValue = Convert.ChangeType(value, property.PropertyType);
                                        propertyAccessor.SetValue(record, property.Name, convertedValue);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // 记录转换失败的详细信息
                                    Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
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
    /// 流式读取记录，适合处理大文件
    /// </summary>
    /// <typeparam name="T">记录类型</typeparam>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>记录流</returns>
    public IEnumerable<T> ReadRecordsStreaming<T>(CancellationToken cancellationToken = default) where T : class, new()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        // 获取类型的属性信息
        var properties = typeof(T).GetProperties();
        var propertyAccessor = new PropertyAccessor<T>(properties);

        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列 - 使用Task.Run避免死锁
                var columns = Task.Run(() => 
                        ReadColumnsAsync(groupReader, fields, cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(),
                    cancellationToken).GetAwaiter().GetResult();

                // 处理每一行数据
                long rowCount = groupReader.RowCount;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
                                    // 使用预编译的属性设置器
                                    if (propertyAccessor.HasProperty(property.Name))
                                    {
                                        var convertedValue = Convert.ChangeType(value, property.PropertyType);
                                        propertyAccessor.SetValue(record, property.Name, convertedValue);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // 记录转换失败的详细信息
                                    Debug.WriteLine($"属性 {property.Name} 值转换失败: {ex.Message}");
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
    /// 异步流式读取记录作为动态对象，适合处理大文件
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>动态对象流</returns>
    public async IAsyncEnumerable<Dictionary<string, object>> ReadRecordsStreamingAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列
                var columns = await ReadColumnsAsync(groupReader, fields, cancellationToken);

                // 处理每一行数据
                long rowCount = groupReader.RowCount;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
    /// 流式读取记录作为动态对象，适合处理大文件
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>动态对象流</returns>
    public IEnumerable<Dictionary<string, object>> ReadRecordsStreaming(CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));

        // 获取所有字段
        var fields = _reader.Schema.Fields.OfType<DataField>().ToArray();

        // 处理每个行组
        for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
            {
                // 读取所有列 - 使用Task.Run避免死锁
                var columns = Task.Run(() => 
                        ReadColumnsAsync(groupReader, fields, cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(), 
                    cancellationToken).GetAwaiter().GetResult();

                // 处理每一行数据
                long rowCount = groupReader.RowCount;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>列数据</returns>
    public Array ReadColumn(string columnName, CancellationToken cancellationToken = default)
    {
        // 使用Task.Run避免死锁
        return Task.Run(() => 
                ReadColumnAsync(columnName, cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(), 
            cancellationToken).GetAwaiter().GetResult();
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
                throw new ParquetParserException($"找不到列: {columnName}", 
                    "ReadColumn", _filePath, new Dictionary<string, object> { { "ColumnName", columnName } });

            // 创建适当大小的数组以保存所有行组的数据
            long totalRowCount = await GetRowCountAsync(cancellationToken);
            var columnType = field.ClrType;
            Array result = Array.CreateInstance(columnType, totalRowCount);
            long currentIndex = 0;

            // 读取每个行组的数据
            for (int groupIndex = 0; groupIndex < _reader.RowGroupCount; groupIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var groupReader = _reader.OpenRowGroupReader(groupIndex))
                {
                    // 读取当前行组的列
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
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException($"异步读取列 {columnName} 操作已取消。", 
                "ReadColumn", _filePath);
        }
        catch (ParquetException pEx)
        {
            var context = new Dictionary<string, object>
            {
                { "ColumnName", columnName },
                { "RowGroupCount", _reader.RowGroupCount }
            };

            throw new ParquetParserException($"Parquet 读取列异常: {pEx.Message}", 
                "ReadColumn", _filePath, context, pEx);
        }
        catch (Exception ex)
        {
            throw new ParquetParserException($"异步读取列 {columnName} 失败: {ex.Message}", 
                "ReadColumn", _filePath, null, ex);
        }
    }

    /// <summary>
    /// 读取指定行组的所有列
    /// </summary>
    private async Task<Dictionary<string, DataColumn>> ReadColumnsAsync(
        ParquetRowGroupReader groupReader,
        DataField[] fields,
        CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, DataColumn>();
        var errorFields = new List<string>();

        foreach (var field in fields)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                // 使用异步方法读取列
                var column = await groupReader.ReadColumnAsync(field);
                columns[field.Name] = column;
            }
            catch (Exception ex)
            {
                // 记录读取失败的字段
                errorFields.Add(field.Name);
                Debug.WriteLine($"读取列 {field.Name} 失败: {ex.Message}");
            }
        }

        // 如果所有字段都读取失败，抛出异常
        if (errorFields.Count == fields.Length)
        {
            throw new ParquetParserException($"所有列读取失败，总计 {errorFields.Count} 个字段", 
                "ReadColumns", null, 
                new Dictionary<string, object> { { "FailedFields", string.Join(", ", errorFields) } });
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
    /// 获取文件元数据
    /// </summary>
    /// <returns>元数据字典</returns>
    public Dictionary<string, string> GetMetadata()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileReader));
                
        return _reader.CustomMetadata;
    }

    /// <summary>
    /// 实现 IEnumerable<Dictionary<string, object>> 接口，允许使用 foreach 遍历记录
    /// </summary>
    public IEnumerator<Dictionary<string, object>> GetEnumerator()
    {
        return ReadRecordsStreaming().GetEnumerator();
    }

    /// <summary>
    /// 实现 IEnumerable 接口，允许使用 foreach 遍历记录
    /// </summary>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// 异步释放资源
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
            return;

        ResourceManager.SafeDispose(_reader);
        await ResourceManager.SafeDisposeWithTimeoutAsync(_fileStream, TimeSpan.FromSeconds(3));
            
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed)
            return;

        ResourceManager.SafeDispose(_reader);
        ResourceManager.SafeDispose(_fileStream);
            
        _isDisposed = true;
        GC.SuppressFinalize(this);
    }
}
