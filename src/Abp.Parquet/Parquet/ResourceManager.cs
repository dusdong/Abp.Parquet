using System.Diagnostics;

namespace Abp.Parquet;

/// <summary>
/// 资源管理助手类，用于安全释放资源
/// </summary>
internal static class ResourceManager
{
    /// <summary>
    /// 安全释放可处置资源的通用方法
    /// </summary>
    public static void SafeDispose(IDisposable resource)
    {
        if (resource == null) return;

        try
        {
            resource.Dispose();
        }
        catch (Exception ex)
        {
            // 记录但不抛出
            Debug.WriteLine($"安全释放资源时发生错误: {ex.Message}");
        }
    }

    /// <summary>
    /// 异步安全释放资源
    /// </summary>
    public static async ValueTask SafeDisposeAsync(IAsyncDisposable resource)
    {
        if (resource == null) return;

        try
        {
            await resource.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // 记录但不抛出
            Debug.WriteLine($"异步安全释放资源时发生错误: {ex.Message}");
        }
    }

    /// <summary>
    /// 带超时的安全资源释放
    /// </summary>
    public static async ValueTask SafeDisposeWithTimeoutAsync(IAsyncDisposable resource, TimeSpan timeout)
    {
        if (resource == null) return;

        using var cts = new CancellationTokenSource(timeout);
        try
        {
            // 创建一个超时任务
            var disposeTask = resource.DisposeAsync().AsTask();
            var timeoutTask = Task.Delay(Timeout.Infinite, cts.Token);

            // 等待资源释放或超时
            var completedTask = await Task.WhenAny(disposeTask, timeoutTask).ConfigureAwait(false);

            if (completedTask == timeoutTask)
            {
                Debug.WriteLine($"资源释放操作超时 ({timeout.TotalMilliseconds}ms)");
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"带超时的资源释放操作失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 安全关闭文件流
    /// </summary>
    public static async ValueTask SafeCloseStreamAsync(Stream stream)
    {
        if (stream == null) return;

        try
        {
            // 尝试刷新缓冲区
            await stream.FlushAsync().ConfigureAwait(false);
            await SafeDisposeAsync(stream).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"关闭流时发生错误: {ex.Message}");
        }
    }
}
