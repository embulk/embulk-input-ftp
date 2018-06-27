package org.embulk.util.ftp;

import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.io.IOException;
import java.io.EOFException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import com.google.common.base.Function;

public class BlockingTransfer
{
    private final WriterChannel writerChannel;
    private final ReaderChannel readerChannel;
    private Future<?> transferCompletionFuture;

    public static BlockingTransfer submit(ExecutorService executor,
            Function<BlockingTransfer, Runnable> starterFactory)
    {
        BlockingTransfer transfer = new BlockingTransfer();
        final Runnable starter = starterFactory.apply(transfer);
        transfer.setTransferCompletionFuture(
                executor.submit(new Callable<Void>() {
                    public Void call() throws Exception
                    {
                        starter.run();
                        return null;
                    }
                })
            );
        return transfer;
    }

    private BlockingTransfer()
    {
        this.writerChannel = new WriterChannel();
        this.readerChannel = new ReaderChannel();
    }

    private void setTransferCompletionFuture(Future<?> future)
    {
        this.transferCompletionFuture = future;
    }

    public ReadableByteChannel getReaderChannel()
    {
        return readerChannel;
    }

    public WritableByteChannel getWriterChannel()
    {
        return writerChannel;
    }

    public void transferFailed(Throwable exception)
    {
        readerChannel.overwriteException(exception);
    }

    void waitForTransferCompletion() throws IOException
    {
        Future<?> f = transferCompletionFuture;
        if (f != null) {
            try {
                f.get();
            } catch (CancellationException ex) {
                throw new InterruptedIOException();
            } catch (InterruptedException ex) {
                throw new InterruptedIOException();
            } catch (ExecutionException ex) {
                // transfer failed
                Throwable e = ex.getCause();
                if (e instanceof IOException) {
                    throw (IOException) e;
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else if (e instanceof Error) {
                    throw (Error) e;
                } else {
                    throw new IOException(e);
                }
            }
        }
    }

    public class WriterChannel implements WritableByteChannel
    {
        public int write(ByteBuffer src) throws IOException
        {
            int sz = src.remaining();
            if (sz <= 0) {
                return sz;
            }

            synchronized(readerChannel) {
                if (!readerChannel.waitForWritable()) {
                    return -1;
                }

                readerChannel.setBuffer(src);

                if (!readerChannel.waitForWritable()) {  // wait for complete processing src
                    return -1;
                }
            }

            return sz - src.remaining();
        }

        public boolean isOpen()
        {
            return readerChannel.isOpen();
        }

        public void close() throws IOException
        {
            readerChannel.closePeer();
            waitForTransferCompletion();
        }
    }

    private static int transferByteBuffer(ByteBuffer src, ByteBuffer dst)
    {
        int pos = dst.position();

        int srcrem = src.remaining();
        int dstrem = dst.remaining();
        if (dstrem < srcrem) {
            int lim = src.limit();
            try {
                src.limit(src.position() + dstrem);
                dst.put(src);
            } finally {
                src.limit(lim);
            }
        } else {
            dst.put(src);
        }

        return dst.position() - pos;
    }

    public class ReaderChannel implements ReadableByteChannel
    {
        private ByteBuffer buffer;
        private Throwable exception;

        public synchronized int read(ByteBuffer dst) throws IOException
        {
            if (!waitForReadable()) {
                return -1;
            }

            int len = transferByteBuffer(buffer, dst);
            if (!buffer.hasRemaining()) {
                setBuffer(null);
                notifyAll();
            }

            return len;
        }

        public synchronized boolean isOpen()
        {
            return exception == null;
        }

        public void close() throws IOException
        {
            setException(new EOFException("reader closed channel"));
        }

        private void setBuffer(ByteBuffer buffer)
        {
            this.buffer = buffer;
            notifyAll();
        }

        private synchronized boolean waitForWritable() throws IOException
        {
            while (buffer != null) {
                if (exception != null) {
                    if (exception instanceof EOFException) {
                        return false;
                    }
                    throwException();
                }

                try {
                    wait();
                } catch (InterruptedException ex) {
                    // TODO throws ClosedByInterruptException or InterruptedIOException?
                }
            }

            return true;
        }

        private boolean waitForReadable() throws IOException
        {
            while(buffer == null) {
                if (exception != null) {
                    if (exception instanceof EOFException) {
                        return false;
                    }
                    throwException();
                }

                try {
                    wait();
                } catch (InterruptedException ex) {
                    // TODO throws ClosedByInterruptException or InterruptedIOException?
                }
            }

            return true;
        }

        public synchronized void closePeer() throws IOException
        {
            waitForWritable();
            if( exception != null && !(exception instanceof EOFException)) {
                throwException();
            }
            setException(new EOFException("writer closed channel"));
        }

        public synchronized void setException(Throwable exception)
        {
            if (this.exception == null) {
                this.exception = exception;
            }
            notifyAll();
        }

        public synchronized void overwriteException(Throwable exception)
        {
            this.exception = exception;
            notifyAll();
        }

        public boolean hasException()
        {
            return exception != null;
        }

        public void throwException() throws IOException
        {
            Throwable ex = exception;
            if (ex instanceof IOException) {
                throw (IOException) ex;
            } else if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else if (ex instanceof Error) {
                throw (Error) ex;
            } else {
                throw new IOException(ex);
            }
        }
    }
}

