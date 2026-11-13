using Downloader.Exceptions;

namespace Downloader.Test.UnitTests;

public class ConcurrentBatchBufferTest(ITestOutputHelper output) : BaseTestClass(output)
{
    private static readonly TimeSpan BlockedReadWriteWaitTime = TimeSpan.FromSeconds(1);

    [Fact(Timeout = 10_000)]
    public async Task IsEmptyPropertyTest()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(0, 999);

        Assert.True(buffer.IsEmpty);

        await buffer.WriteAsync(new(1, 1, 1));
        Assert.False(buffer.IsEmpty);

        var storage = await buffer.SwapAsync(null);
        Assert.True(buffer.IsEmpty);

        storage = await buffer.SwapAsync(storage);
        Assert.True(buffer.IsEmpty);
    }

    [Fact(Timeout = 10_000)]
    public async Task SwappedStorageIsCleared()
    {
        ConcurrentBatchBuffer<Data>.Storage storage = null;
        var data = new Data(0, 1, 1);
        var buffer = new ConcurrentBatchBuffer<Data>(0, 9999);

        await buffer.WriteAsync(data);
        storage = await buffer.SwapAsync(storage);
        Assert.Contains(data, storage);

        await buffer.WriteAsync(new(2, 2, 2));
        storage = await buffer.SwapAsync(storage);
        Assert.DoesNotContain(data, storage);

        await buffer.WriteAsync(new(3, 3, 3));
        storage = await buffer.SwapAsync(storage);
        Assert.DoesNotContain(data, storage);
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadBlocked_WhenEmpty_ThenWokenByWrite()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(0, 9999);
        ConcurrentBatchBuffer<Data>.Storage storage = null;

        var swapTask = buffer.SwapAsync(storage);
        Assert.False(swapTask.IsCompleted);

        await buffer.WriteAsync(new(1, 1, 1));
        await Task.Delay(BlockedReadWriteWaitTime);
        Assert.True(swapTask.IsCompletedSuccessfully);

        storage = await swapTask;
        swapTask = buffer.SwapAsync(storage);
        Assert.False(swapTask.IsCompleted);

        await buffer.WriteAsync(new(2, 2, 2));
        await Task.Delay(BlockedReadWriteWaitTime);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadBlocked_ThenWokenIfBatchThresholdReached()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(16, 9999);

        await buffer.WriteAsync(new(0, 4, 4));
        var swapTask = buffer.SwapAsync(null);
        Assert.False(swapTask.IsCompleted);

        foreach (var _ in Enumerable.Repeat(0, 3))
        {
            await buffer.WriteAsync(new(0, 4, 4));
        }
        await Task.Delay(BlockedReadWriteWaitTime);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadBlocked_ThenWokenIfSizeLimitReached()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(4, 32);

        await buffer.WriteAsync(new(0, 8, 3));
        var swapTask = buffer.SwapAsync(null);
        Assert.False(swapTask.IsCompleted);

        foreach (int batch in Enumerable.Range(1, 2))
        {
            await buffer.WriteAsync(new(batch, 8, 3));
            await Task.Delay(BlockedReadWriteWaitTime);
            Assert.False(swapTask.IsCompleted);
        }

        await buffer.WriteAsync(new(3, 8, 3));
        await Task.Delay(BlockedReadWriteWaitTime);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadUnblocked_IfBatchThresholdReached()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(12, 9999);

        foreach (int batch in Enumerable.Repeat(0, 3))
        {
            await buffer.WriteAsync(new(batch, 4, 4));
        }

        var swapTask = buffer.SwapAsync(null);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task ReadUnblocked_IfSizeLimitReached()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(4, 12);

        foreach (int batch in Enumerable.Repeat(0, 3))
        {
            await buffer.WriteAsync(new(batch, 4, 0));
        }

        var swapTask = buffer.SwapAsync(null);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Theory(Timeout = 10_000)]
    [InlineData(0, 12, 4, 0, 3)]
    public async Task WriteBlocked_WhenSizeLimitReached_ThenWokenByRead(int batchThreshold, int capacity, int itemSize, int itemContentSize, int itemCountUnblocked)
    {
        var buffer = new ConcurrentBatchBuffer<Data>(batchThreshold, capacity);

        for (int batch = 0; batch < itemCountUnblocked; batch++)
        {
            await buffer.WriteAsync(new(batch, itemSize, itemContentSize));
        }
        var writeTask1 = buffer.WriteAsync(new(0, itemSize, itemContentSize));
        Assert.False(writeTask1.IsCompleted);
        var writeTask2 = buffer.WriteAsync(new(999, itemSize, itemContentSize));
        Assert.False(writeTask2.IsCompleted);

        await buffer.SwapAsync(null);
        await Task.Delay(BlockedReadWriteWaitTime);
        Assert.True(writeTask1.IsCompletedSuccessfully);
        Assert.True(writeTask2.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task WriteUnblocked_IfSizeLimitNotReached()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(0, 3);

        foreach (int batch in Enumerable.Repeat(0, 3))
        {
            var writeTask = buffer.WriteAsync(new(batch, 1, 1));
            Assert.True(writeTask.IsCompletedSuccessfully);
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task WriteForceReadableTest()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);

        await buffer.WriteAsync(new(0, 1, 1));
        var swapTask = buffer.SwapAsync(null);
        Assert.False(swapTask.IsCompleted);

        await buffer.WriteAsync(new(1, 1, 1), true);
        await Task.Delay(BlockedReadWriteWaitTime);

        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_ReadAlwaysUnblocked()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);

        await buffer.WriteAsync(new(0, 1, 1));
        buffer.Close();

        var swapTask = buffer.SwapAsync(null);
        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_ReadThrowIfEmpty()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);
        buffer.Close();
        await Assert.ThrowsAsync<BufferClosedException>(async () => await buffer.SwapAsync(null));

        buffer = new(999, 999);
        await buffer.WriteAsync(new(0, 1, 1));
        buffer.Close();
        var storage = await buffer.SwapAsync(null);
        await Assert.ThrowsAsync<BufferClosedException>(async () => await buffer.SwapAsync(storage));
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_BlockedReadWake()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);

        await buffer.WriteAsync(new(0, 1, 1));
        var swapTask = buffer.SwapAsync(null);
        buffer.Close();
        await Task.Delay(BlockedReadWriteWaitTime);

        Assert.True(swapTask.IsCompletedSuccessfully);
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_BlockedReadThrowIfEmpty()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);

        var swapTask = buffer.SwapAsync(null);
        buffer.Close();

        await Assert.ThrowsAsync<BufferClosedException>(async () => await swapTask);
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_WriteThrow()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(999, 999);

        buffer.Close();

        await Assert.ThrowsAsync<BufferClosedException>(async () => await buffer.WriteAsync(new(0, 1, 1)));
    }

    [Fact(Timeout = 10_000)]
    public async Task AfterClosed_BlockedWriteThrow()
    {
        var buffer = new ConcurrentBatchBuffer<Data>(1, 1);

        await buffer.WriteAsync(new(0, 1, 1));
        var writeTask = buffer.WriteAsync(new(0, 99, 99));
        buffer.Close();

        await Assert.ThrowsAsync<BufferClosedException>(async () => await writeTask);
    }

    [InlineData(0, 0, 0, 0)]
    [InlineData(0, 0, 1, 1)]
    [InlineData(16, 64, 8, 4)]
    [InlineData(16, 64, 8, 8)]
    [InlineData(16, 64, 32, 8)]
    [Theory(Timeout = 60_000)]
    public async Task ReadWriteConsistencyAndCloseableTest(int batchThreshold, int capacity, int itemSize, int itemContentSize)
    {
        const int BatchCount = 60;

        var srcDict = new Dictionary<int, Data[]>();
        for (int batch = 1; batch <= BatchCount; batch++)
        {
            srcDict[batch] = Enumerable.Repeat(batch, batch)
                .Select(batch => new Data(batch, itemSize, itemContentSize))
                .ToArray();
        }
        var srcList = new List<Data>();
        for (int batch = 1; batch <= BatchCount; batch++)
        {
            srcList.AddRange(srcDict[batch]);
        }
        var dstList = new List<Data>();
        var buffer = new ConcurrentBatchBuffer<Data>(batchThreshold, capacity);

        var consumer = new Consumer(buffer, dstList.Add);
        var producerDict = new Dictionary<int, Task>();
        for (int batch = 1; batch <= BatchCount; batch++)
        {
            var items = srcDict[batch];
            producerDict[batch] = Task.Run(async () => 
            {
                foreach (var item in items)
                {
                    await buffer.WriteAsync(item).ConfigureAwait(false);
                }
            });
        }
        await Task.WhenAll(producerDict.Values);
        buffer.Close();
        await consumer.Task;

        Assert.Equal(srcList, dstList, ReferenceEquals);
        Assert.True(buffer.IsClosed);
    }

    private class Data : IMeasurableBatchData
    {
        public Data(long batchNumber)
            : this(batchNumber, 0, 0)
        {
        }

        public Data(long batchNumber, long size, long contentSize)
            : this(batchNumber, size, contentSize, null)
        {
        }

        public Data(long batchNumber, long size, long contentSize, TaskCompletionSource completionSource)
        {
            BatchNumber = batchNumber;
            Size = size;
            ContentSize = contentSize;
            CompletionSource = completionSource;
        }

        public Data(TaskCompletionSource completionSource)
            : this(-1, 0, 0, completionSource)
        {
        }

        public long BatchNumber { get; set; }

        public long Size { get; set; }

        public long ContentSize { get; set; }

        public TaskCompletionSource CompletionSource { get; set; }

        public static Data CreateCompletionSource()
            => new(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    private class Consumer
    {
        private readonly ConcurrentBatchBuffer<Data> _buffer;
        private ConcurrentBatchBuffer<Data>.Storage _storage;
        private readonly Action<Data> _dataCallback;
        private readonly TaskCompletionSource _completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Consumer(ConcurrentBatchBuffer<Data> buffer, Action<Data> dataCallback, CancellationToken cancellationToken = default)
        {
            _buffer = buffer;
            _dataCallback = dataCallback;

            Task.Run(async () => {
                while (true)
                {
                    try
                    {
                        _storage = await _buffer.SwapAsync(_storage, cancellationToken).ConfigureAwait(false);
                        foreach (var data in _storage)
                        {
                            if (data.CompletionSource is not null)
                            {
                                data.CompletionSource.SetResult();
                            }
                            else
                            {
                                _dataCallback(data);
                            }
                        }
                    }
                    catch (BufferClosedException)
                    {
                        break;
                    }
                    catch (OperationCanceledException ex)
                    {
                        _completionSource.SetCanceled(ex.CancellationToken);
                        break;
                    }
                    catch (Exception ex)
                    {
                        _completionSource.SetException(ex);
                        break;
                    }
                }
            });
        }

        public Task Task => _completionSource.Task;

        public async Task DrainAsync(CancellationToken cancellationToken = default)
        {
            var drain = Data.CreateCompletionSource();
            try
            {
                await _buffer.WriteAsync(drain, true, cancellationToken).ConfigureAwait(false);
            }
            catch (BufferClosedException)
            {
                return;
            }
            await Task.WhenAny(
                drain.CompletionSource.Task.WaitAsync(cancellationToken),
                _completionSource.Task.ContinueWith(_ => { }))
                .ConfigureAwait(false);
        }
    }
}
