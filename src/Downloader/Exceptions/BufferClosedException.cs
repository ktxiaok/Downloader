namespace Downloader.Exceptions;

internal class BufferClosedException : Exception
{
    public BufferClosedException()
    {
    }

    public BufferClosedException(string message) : base(message)
    {
    }
}
