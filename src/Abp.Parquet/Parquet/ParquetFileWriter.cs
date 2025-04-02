using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using Abp.Parquet.MemoryManagement;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Abp.Parquet;

/// <summary>
/// 负责将数据写入 Parquet 文件，支持自动生成架构和追加写入。
/// </summary>
/// <typeparam name="T">记录类型。</typeparam>
public class ParquetFileWriter<T> : IDisposable, IAsyncDisposable
{
    // 默认分块大小
    private const int DefaultBatchSize = 10000;

    private readonly string _filePath;
    private readonly ParquetSchema _schema;
    private readonly ParquetWriterConfiguration _configuration;
    private readonly PropertyInfo[] _properties;
    private readonly Dictionary<string, Func<object, object>> _propertyGetters;
    private readonly string _instanceId;

    // 字段名称与原始属性类型的映射
    private readonly Dictionary<string, Type> _fieldOriginalTypes;

    // 字段名称到属性的映射，缓存以避免重复查找
    private readonly Dictionary<string, PropertyInfo> _propertyCache = new Dictionary<string, PropertyInfo>();

    // 持久化的 FileStream 和 ParquetWriter
    private FileStream _fileStream;
    private ParquetWriter _parquetWriter;
    private bool _isDisposed = false;

    // 用于懒初始化的信号量
    private readonly SemaphoreSlim _initSemaphore = new SemaphoreSlim(1, 1);
    private bool _isInitialized = false;

    // 内存管理
    private MemoryTracker _memoryTracker;
    private readonly BufferManager _bufferManager;

    /// <summary>
    /// 私有构造函数，由CreateAsync工厂方法调用
    /// </summary>
    private ParquetFileWriter(string filePath, ParquetSchema schema = null, ParquetWriterConfiguration configuration = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("文件路径不能为空。", nameof(filePath));

        _filePath = filePath;
        _configuration = configuration ?? new ParquetWriterConfiguration();
        _instanceId = Guid.NewGuid().ToString("N");
        _bufferManager = new BufferManager(_configuration.EnableMemoryManagement);
        _propertyGetters = new Dictionary<string, Func<object, object>>();

        // 初始化内存管理器
        if (_configuration.EnableMemoryManagement)
        {
            var memoryManager = ParquetMemoryManager.Instance;
            memoryManager.EnableMemoryLimit = _configuration.EnableMemoryLimit;
            memoryManager.MemoryLimit = _configuration.MemoryLimit;
            _memoryTracker = memoryManager.GetTracker($"ParquetFileWriter_{_instanceId}");
        }

        // 注册自定义类型转换器
        if (_configuration.EnableAutomaticTypeConversion)
        {
            _configuration.RegisterCustomTypeConverters();
        }

        // 架构和字段类型处理
        if (schema != null)
        {
            _schema = schema;
            _fieldOriginalTypes = ExtractFieldTypesFromSchema(_schema);
        }
        else
        {
            var schemaGenerator = new ParquetSchemaGenerator<T>(_configuration);
            _schema = schemaGenerator.GenerateSchema();
            _fieldOriginalTypes = new Dictionary<string, Type>();

            // 获取字段的原始属性类型
            foreach (var field in _schema.Fields.OfType<DataField>())
            {
                string fieldName = field.Name;
                Type originalType = schemaGenerator.GetOriginalFieldType(fieldName);
                _fieldOriginalTypes[fieldName] = originalType;
            }
        }

        // 获取类型 T 的所有公共属性
        _properties = _configuration.EnableReflectionCache
            ? ReflectionHelper.GetProperties(typeof(T))
            : typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // 预构建属性缓存和属性访问器
        BuildPropertyCache();
    }

    /// <summary>
    /// 预构建属性缓存和高性能属性访问器
    /// </summary>
    private void BuildPropertyCache()
    {
        foreach (var field in _schema.Fields.OfType<DataField>())
        {
            string fieldName = field.Name;

            PropertyInfo property = null;

            if (_configuration.EnableReflectionCache)
            {
                var propertyDict = ReflectionHelper.GetPropertyDictionary(typeof(T));
                propertyDict.TryGetValue(fieldName, out property);
            }
            else
            {
                // 查找属性（不区分大小写）
                property = _properties.FirstOrDefault(p => string.Equals(p.Name, fieldName, StringComparison.OrdinalIgnoreCase));
            }

            if (property != null)
            {
                _propertyCache[fieldName] = property;

                // 创建高性能属性访问器
                if (_configuration.EnableReflectionCache)
                {
                    _propertyGetters[fieldName] = ReflectionHelper.GetPropertyGetter(property);
                }
            }
        }
    }

    /// <summary>
    /// 创建 ParquetFileWriter 的异步工厂方法
    /// </summary>
    /// <param name="filePath">目标 Parquet 文件路径</param>
    /// <param name="schema">可选的 Parquet 架构，如果为 null，则自动生成</param>
    /// <param name="configuration">可选的配置对象，如果为 null，则使用默认配置</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>初始化完成的 ParquetFileWriter 实例</returns>
    public static async Task<ParquetFileWriter<T>> CreateAsync(
        string filePath,
        ParquetSchema schema = null,
        ParquetWriterConfiguration configuration = null,
        CancellationToken cancellationToken = default)
    {
        var writer = new ParquetFileWriter<T>(filePath, schema, configuration);
        await writer.InitializeAsync(cancellationToken).ConfigureAwait(false);
        return writer;
    }

    /// <summary>
    /// 异步初始化 FileStream 和 ParquetWriter。
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    private async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_isInitialized) return;

        await _initSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_isInitialized) return;

            cancellationToken.ThrowIfCancellationRequested();

            var fileExists = File.Exists(_filePath);

            try
            {
                // Ensure directory exists
                var directory = Path.GetDirectoryName(_filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Build FileOptions
                FileOptions options = FileOptions.None;
                if (_configuration.UseAsyncIO) options |= FileOptions.Asynchronous;
                if (_configuration.UseSequentialScan) options |= FileOptions.SequentialScan;

                // Open FileStream - this is the true asynchronous IO operation
                _fileStream = new FileStream(
                    _filePath,
                    fileExists ? FileMode.Open : FileMode.Create,
                    FileAccess.ReadWrite,
                    FileShare.Read,
                    _configuration.BufferSize,
                    options);

                if (fileExists)
                {
                    // If append mode, ensure stream pointer is at the end of the file
                    _fileStream.Seek(0, SeekOrigin.End);
                }

                cancellationToken.ThrowIfCancellationRequested();

                // Use retry strategy to create ParquetWriter
                int retryCount = 0;
                Exception lastException = null;

                while (retryCount < _configuration.RetryCount)
                {
                    try
                    {
                        // Create ParquetWriter - using the asynchronous method
                        _parquetWriter = await ParquetWriter.CreateAsync(_schema, _fileStream, append: fileExists, cancellationToken: cancellationToken);

                        _parquetWriter.CompressionMethod = _configuration.CompressionMethod;
                        _parquetWriter.CompressionLevel = _configuration.CompressionLevel;

                        if (_configuration.CustomMetadata != null && _configuration.CustomMetadata.Count > 0)
                            _parquetWriter.CustomMetadata = _configuration.CustomMetadata;

                        break; // Successfully created, exit loop
                    }
                    catch (OperationCanceledException)
                    {
                        // Handle cancellation properly
                        throw;
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        retryCount++;

                        if (retryCount >= _configuration.RetryCount)
                            throw; // Reached maximum retry count, re-throw exception

                        // Wait for a while before retrying
                        await Task.Delay(_configuration.RetryDelayMs * retryCount, cancellationToken);
                    }
                }

                if (_parquetWriter == null && lastException != null)
                {
                    throw lastException; // All retries failed
                }

                _isInitialized = true;
            }
            catch (Exception ex)
            {
                // Ensure resources are released on initialization failure
                await CleanupResourcesAsync();

                if (ex is IOException ioEx)
                {
                    throw new ParquetWriterException($"Initializing Parquet writer failed with I/O error: {ioEx.Message}", _filePath, ioEx);
                }

                throw new ParquetWriterException($"Initializing Parquet writer failed: {ex.Message}", _filePath, ex);
            }
        }
        finally
        {
            _initSemaphore.Release();
        }
    }

    /// <summary>
    /// 清理资源
    /// </summary>
    private async Task CleanupResourcesAsync()
    {
        if (_parquetWriter != null)
        {
            try
            {
                _parquetWriter.Dispose();
                _parquetWriter = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"释放 ParquetWriter 时发生错误: {ex.Message}");
            }
        }

        if (_fileStream != null)
        {
            try
            {
                await _fileStream.DisposeAsync().ConfigureAwait(false);
                _fileStream = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"释放 FileStream 时发生错误: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// 从 ParquetSchema 中提取字段原始类型
    /// </summary>
    private static Dictionary<string, Type> ExtractFieldTypesFromSchema(ParquetSchema schema)
    {
        var fieldTypes = new Dictionary<string, Type>();
        foreach (var field in schema.Fields.OfType<DataField>())
        {
            // 如果是 Nullable<>, 获取基础类型；否则直接使用 ClrType
            Type baseType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
            fieldTypes[field.Name] = baseType;
        }

        return fieldTypes;
    }

    /// <summary>
    /// 写入数据到 Parquet 文件 (同步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    public void Write(List<T> records)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化 - 同步等待异步初始化
        InitializeAsync().GetAwaiter().GetResult();

        try
        {
            // 如果数据量很大，分批处理
            if (records.Count > _configuration.BatchSize)
            {
                WriteBatched(records);
                return;
            }

            // 创建行组
            using var rowGroupWriter = _parquetWriter.CreateRowGroup();

            // 构建字段名称到 DataField 的字典
            var dataFieldDict = _schema.Fields
                .OfType<DataField>()
                .ToDictionary(f => f.Name, f => f);

            // 创建 DataColumn 列表
            var dataColumns = CreateDataColumns(records, dataFieldDict);

            // 写入所有列
            foreach (var dataColumn in dataColumns)
            {
                // 使用同步等待异步操作
                rowGroupWriter.WriteColumnAsync(dataColumn).GetAwaiter().GetResult();
            }

            // 刷新流，确保数据写入
            _fileStream.Flush();
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"写入Parquet数据失败: {ex.Message}", _filePath, ex);
        }
    }

    /// <summary>
    /// 分批写入大量记录 (同步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    private void WriteBatched(List<T> records)
    {
        int batchSize = _configuration.BatchSize;
        int totalRecords = records.Count;

        // 计算需要多少批次
        int batchCount = (totalRecords + batchSize - 1) / batchSize;

        for (int i = 0; i < batchCount; i++)
        {
            int skip = i * batchSize;
            int take = Math.Min(batchSize, totalRecords - skip);

            // 获取当前批次的数据
            var batch = records.GetRange(skip, take);

            // 写入批次数据
            using var rowGroupWriter = _parquetWriter.CreateRowGroup();

            // 构建字段名称到 DataField 的字典
            var dataFieldDict = _schema.Fields
                .OfType<DataField>()
                .ToDictionary(f => f.Name, f => f);

            // 自动创建 DataColumn 列表
            var dataColumns = CreateDataColumns(batch, dataFieldDict);

            // 写入所有列
            foreach (var dataColumn in dataColumns)
            {
                rowGroupWriter.WriteColumnAsync(dataColumn).GetAwaiter().GetResult();
            }
        }

        // 最后刷新流，确保所有数据写入
        _fileStream.Flush();
    }

    /// <summary>
    /// 写入数据到 Parquet 文件 (异步版本)
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    public async Task WriteAsync(List<T> records, CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(ParquetFileWriter<T>));

        if (records == null || !records.Any())
            return;

        // 确保初始化
        await InitializeAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // 如果数据量很大，分批处理
            if (records.Count > _configuration.BatchSize)
            {
                await WriteBatchedAsync(records, cancellationToken).ConfigureAwait(false);
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();

            // 创建行组
            var rowGroupWriter = _parquetWriter.CreateRowGroup();
            using (rowGroupWriter)
            {
                // 构建字段名称到 DataField 的字典
                var dataFieldDict = _schema.Fields
                    .OfType<DataField>()
                    .ToDictionary(f => f.Name, f => f);

                // 是否使用并行处理
                var useParallel = ShouldUseParallelProcessing(records.Count);
                List<DataColumn> dataColumns;

                if (useParallel)
                {
                    // 并行创建 DataColumn 列表
                    dataColumns = await CreateDataColumnsParallelAsync(records, dataFieldDict, cancellationToken);
                }
                else
                {
                    // 自动创建 DataColumn 列表 - 这是CPU密集型的，可以考虑在后台线程执行
                    dataColumns = await Task.Run(() => CreateDataColumns(records, dataFieldDict), cancellationToken);
                }

                // 写入所有列 - 使用正确的异步方法，并传递取消令牌
                foreach (var dataColumn in dataColumns)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await rowGroupWriter.WriteColumnAsync(dataColumn);
                }
            }

            // 异步刷新流
            await _fileStream.FlushAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw new ParquetOperationCanceledException("Parquet 写入操作已被取消。");
        }
        catch (Exception ex)
        {
            throw new ParquetWriterException($"异步写入Parquet数据失败: {ex.Message}", _filePath, ex);
        }
    }

    /// <summary>
    /// 分批异步写入大量记录
    /// </summary>
    /// <param name="records">要写入的数据列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>完成任务</returns>
    private async Task WriteBatchedAsync(List<T> records, CancellationToken cancellationToken)
    {
        int batchSize = _configuration.BatchSize;
        int totalRecords = records.Count;

        // 计算需要多少批次
        int batchCount = (totalRecords + batchSize - 1) / batchSize;

        for (int i = 0; i < batchCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int skip = i * batchSize;
            int take = Math.Min(batchSize, totalRecords - skip);

            // 获取当前批次的数据
            var batch = records.GetRange(skip, take);

            // 写入批次数据
            var rowGroupWriter = _parquetWriter.CreateRowGroup();
            using (rowGroupWriter)
            {
                // 构建字段名称到 DataField 的字典
                var dataFieldDict = _schema.Fields
                    .OfType<DataField>()
                    .ToDictionary(f => f.Name, f => f);

                // 是否使用并行处理
                var useParallel = ShouldUseParallelProcessing(batch.Count);
                List<DataColumn> dataColumns;

                if (useParallel)
                {
                    // 并行创建 DataColumn 列表
                    dataColumns = await CreateDataColumnsParallelAsync(batch, dataFieldDict, cancellationToken);
                }
                else
                {
                    // 自动创建 DataColumn 列表
                    dataColumns = await Task.Run(() => CreateDataColumns(batch, dataFieldDict), cancellationToken);
                }

                // 写入所有列
                foreach (var dataColumn in dataColumns)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await rowGroupWriter.WriteColumnAsync(dataColumn);
                }
            }
        }

        // 最后异步刷新流，确保所有数据写入
        await _fileStream.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// 判断是否应该使用并行处理
    /// </summary>
    private bool ShouldUseParallelProcessing(int recordCount)
    {
        // 如果配置不允许，直接返回false
        if (!_configuration.EnableParallelProcessing)
            return false;

        // 如果记录数小于最小阈值，不使用并行
        if (recordCount < _configuration.MinimumRecordsForParallel)
            return false;

        // 如果启用了内存限制，并且可用内存不足，考虑不使用并行
        if (_configuration.EnableMemoryLimit && _configuration.AutoAdjustParallelism)
        {
            var memoryManager = ParquetMemoryManager.Instance;
            long availableMemory = _configuration.MemoryLimit - memoryManager.AllocatedMemory;

            // 估计每条记录的内存使用
            long estimatedRecordSize = 1024; // 假设每条记录1KB

            // 如果可用内存少于记录总数估计大小的2倍，不使用并行以避免内存压力
            if (availableMemory < recordCount * estimatedRecordSize * 2)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 并行创建DataColumns列表，适用于大量记录处理
    /// </summary>
    private async Task<List<DataColumn>> CreateDataColumnsParallelAsync(
        List<T> records,
        Dictionary<string, DataField> dataFieldDict,
        CancellationToken cancellationToken)
    {
        var dataFields = _schema.Fields.OfType<DataField>().ToList();
        var recordsCount = records.Count;
        var result = new List<DataColumn>(dataFields.Count);

        // 限制并行度
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = _configuration.MaxDegreeOfParallelism,
            CancellationToken = cancellationToken
        };

        // 创建一个并发字典来存储结果
        var resultColumns = new ConcurrentDictionary<int, DataColumn>();

        // 使用Parallel.ForEach高效地处理每个字段
        await Task.Run(() =>
        {
            Parallel.ForEach(Enumerable.Range(0, dataFields.Count), parallelOptions, index =>
            {
                var field = dataFields[index];
                string fieldName = field.Name;

                try
                {
                    // 使用单字段处理方法创建单个DataColumn
                    var dataColumn = CreateDataColumnForField(records, field, fieldName, recordsCount);
                    if (dataColumn != null)
                    {
                        resultColumns.TryAdd(index, dataColumn);
                    }
                }
                catch (Exception ex)
                {
                    throw new ParquetWriterException($"并行处理字段 '{fieldName}' 时发生错误: {ex.Message}", ex);
                }
            });
        }, cancellationToken);

        // 按索引顺序添加列
        for (int i = 0; i < dataFields.Count; i++)
        {
            if (resultColumns.TryGetValue(i, out var column))
            {
                result.Add(column);
            }
        }

        return result;
    }

    /// <summary>
    /// 为单个字段创建DataColumn
    /// </summary>
    private DataColumn CreateDataColumnForField(List<T> records, DataField field, string fieldName, int recordsCount)
    {
        // 检查属性是否存在
        if (!_propertyCache.TryGetValue(fieldName, out var property))
        {
            return null; // 忽略不存在的属性
        }

        // 获取属性原始类型
        if (!_fieldOriginalTypes.TryGetValue(fieldName, out Type originalPropertyType))
        {
            return null; // 忽略未知类型的属性
        }

        // 使用高性能属性访问器或直接反射
        object[] rawValues = new object[recordsCount];

        if (_configuration.EnableReflectionCache && _propertyGetters.TryGetValue(fieldName, out var getter))
        {
            // 使用缓存的高性能访问器
            for (int i = 0; i < recordsCount; i++)
            {
                rawValues[i] = getter(records[i]);
            }
        }
        else
        {
            // 使用直接反射
            for (int i = 0; i < recordsCount; i++)
            {
                rawValues[i] = property.GetValue(records[i]);
            }
        }

        return ProcessFieldValues(field, fieldName, originalPropertyType, rawValues, recordsCount);
    }

    /// <summary>
    /// 处理字段值并创建DataColumn
    /// </summary>
    private DataColumn ProcessFieldValues(DataField field, string fieldName, Type originalPropertyType, object[] rawValues, int recordCount)
    {
        try
        {
            // 确定目标数组类型和创建适当的数组
            Array fieldValues;
            Type fieldElementType = field.IsNullable ? Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType : field.ClrType;

            if (field.IsNullable)
            {
                // 处理可空字段
                if (originalPropertyType.IsValueType && Nullable.GetUnderlyingType(originalPropertyType) == null)
                {
                    // 处理非可空值类型到可空类型的转换 (例如: DateTime[] -> DateTime?[])

                    // 创建强类型源数组
                    Array typedSourceArray = Array.CreateInstance(originalPropertyType, recordCount);
                    for (int i = 0; i < recordCount; i++)
                    {
                        if (rawValues[i] != null)
                        {
                            typedSourceArray.SetValue(rawValues[i], i);
                        }
                    }

                    // 尝试使用通用转换器
                    if (_configuration.EnableAutomaticTypeConversion &&
                        ParquetTypeConverterRegistry.Instance.TryConvert(typedSourceArray, field.ClrType, out var convertedArray))
                    {
                        fieldValues = convertedArray;
                    }
                    else
                    {
                        // 创建目标可空数组
                        Type nullableType = typeof(Nullable<>).MakeGenericType(fieldElementType);
                        fieldValues = Array.CreateInstance(nullableType, recordCount);

                        // 填充数组
                        for (int i = 0; i < recordCount; i++)
                        {
                            if (rawValues[i] != null)
                            {
                                // 创建可空类型实例
                                var nullableValue = Activator.CreateInstance(nullableType, rawValues[i]);
                                fieldValues.SetValue(nullableValue, i);
                            }
                            else
                            {
                                fieldValues.SetValue(null, i);
                            }
                        }
                    }
                }
                else if (Nullable.GetUnderlyingType(originalPropertyType) != null)
                {
                    // 已经是可空类型，直接使用
                    fieldValues = Array.CreateInstance(originalPropertyType, recordCount);
                    for (int i = 0; i < recordCount; i++)
                    {
                        fieldValues.SetValue(rawValues[i], i);
                    }
                }
                else
                {
                    // 引用类型处理
                    fieldValues = rawValues.Select(val => val != null ? val.ToString() : null).ToArray();
                }
            }
            else
            {
                // 处理非可空字段
                fieldValues = Array.CreateInstance(field.ClrType, recordCount);

                for (int i = 0; i < recordCount; i++)
                {
                    if (rawValues[i] != null)
                    {
                        try
                        {
                            // 转换原始值到目标类型
                            fieldValues.SetValue(Convert.ChangeType(rawValues[i], field.ClrType), i);
                        }
                        catch (Exception ex)
                        {
                            throw new ParquetTypeConversionException(
                                $"转换类型失败: {rawValues[i]} -> {field.ClrType.Name}",
                                rawValues[i]?.GetType(), field.ClrType, fieldName, ex);
                        }
                    }
                    else
                    {
                        // 使用默认值避免null
                        fieldValues.SetValue(GetDefault(field.ClrType), i);
                    }
                }
            }

            // 创建并返回DataColumn
            return new DataColumn(field, fieldValues);
        }
        catch (Exception ex)
        {
            var message = $"处理字段 '{fieldName}' 时出错: {ex.Message}";
            if (ex is ArgumentException argEx && argEx.Message.Contains("expected") && argEx.Message.Contains("but passed"))
            {
                message = $"类型不匹配: {argEx.Message}, 字段: {fieldName}, 原始类型: {originalPropertyType.Name}, 目标类型: {field.ClrType.Name}";
            }

            throw new ParquetWriterException(message, ex);
        }
    }

    /// <summary>
    /// 为所有字段创建 DataColumn 列表
    /// </summary>
    private List<DataColumn> CreateDataColumns(List<T> records, Dictionary<string, DataField> dataFieldDict)
    {
        var dataColumns = new List<DataColumn>();
        int recordCount = records.Count;

        foreach (var field in _schema.Fields.OfType<DataField>())
        {
            try
            {
                var dataColumn = CreateDataColumnForField(records, field, field.Name, recordCount);
                if (dataColumn != null)
                {
                    dataColumns.Add(dataColumn);
                }
            }
            catch (Exception ex)
            {
                throw new ParquetWriterException($"处理字段 '{field.Name}' 时发生错误: {ex.Message}", ex);
            }
        }

        return dataColumns;
    }

    /// <summary>
    /// 获取指定类型的默认值
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object GetDefault(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (_isDisposed) return;

        _isDisposed = true;

        try
        {
            _parquetWriter?.Dispose();
            _fileStream?.Dispose();
            _bufferManager?.Dispose();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"释放资源时发生错误: {ex.Message}");
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 异步释放资源
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;

        _isDisposed = true;

        try
        {
            _parquetWriter?.Dispose();
            if (_fileStream != null)
                await _fileStream.DisposeAsync().ConfigureAwait(false);
            _bufferManager?.Dispose();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"异步释放资源时发生错误: {ex.Message}");
        }

        GC.SuppressFinalize(this);
    }
}
