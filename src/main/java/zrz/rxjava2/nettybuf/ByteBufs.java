package zrz.rxjava2.nettybuf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public class ByteBufs {

  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final ByteBufAllocator DEFAULT_BYTEBUF_ALLOCATOR = ByteBufAllocator.DEFAULT;

  /**
   * returns a function which will convert each bytebuf passed to it using a converter, and then immediately release it.
   * 
   * @param converter
   * @return
   */
  public static <T> Function<ByteBuf, T> releasedMap(Function<ByteBuf, T> converter) {
    return in -> {
      try {
        return converter.apply(in);
      }
      finally {
        in.release();
      }
    };
  }

  /**
   * an adapter to convert an input stream into a stream of bytebufs.
   * 
   * @param in
   * @param bufferSize
   * @param alloc
   * @return
   */

  public static Flowable<ByteBuf> from(final InputStream in, ByteBufAllocator alloc, final int bufferSize) {

    return Flowable.generate(new Consumer<Emitter<ByteBuf>>() {

      @Override
      public void accept(Emitter<ByteBuf> emitter) throws Exception {

        ByteBuf buffer = alloc.buffer(bufferSize);

        int count = buffer.writeBytes(in, bufferSize);

        if (count == -1) {
          emitter.onComplete();
        }
        else {
          emitter.onNext(buffer);
        }

      }

    });
  }

  public static Flowable<ByteBuf> from(final InputStream in, ByteBufAllocator alloc) {
    return from(in, alloc, DEFAULT_BUFFER_SIZE);
  }

  public static Flowable<ByteBuf> from(final InputStream in, int bufferSize) {
    return from(in, DEFAULT_BYTEBUF_ALLOCATOR, DEFAULT_BUFFER_SIZE);
  }

  public static Flowable<ByteBuf> from(final InputStream in) {
    return from(in, DEFAULT_BYTEBUF_ALLOCATOR, DEFAULT_BUFFER_SIZE);
  }

  /**
   * read bytebufs from a file, or returns an error flowable if the file does not exist.
   * 
   * @param file
   * @return
   */

  @SuppressWarnings("resource")
  public static Flowable<ByteBuf> read(final Path file, ByteBufAllocator alloc, int bufferSize) {
    FileInputStream in;
    try {
      in = new FileInputStream(file.toFile());
    }
    catch (FileNotFoundException e) {
      return Flowable.error(e);
    }
    return from(in, alloc, bufferSize).doAfterTerminate(in::close);
  }

  public static Flowable<ByteBuf> read(final Path file, int bufferSize) {
    return read(file, DEFAULT_BYTEBUF_ALLOCATOR, bufferSize);
  }

  public static Flowable<ByteBuf> read(final Path file, ByteBufAllocator alloc) {
    return read(file, alloc, DEFAULT_BUFFER_SIZE);
  }

  public static Flowable<ByteBuf> read(final Path file) {
    return read(file, DEFAULT_BYTEBUF_ALLOCATOR, DEFAULT_BUFFER_SIZE);
  }

}
