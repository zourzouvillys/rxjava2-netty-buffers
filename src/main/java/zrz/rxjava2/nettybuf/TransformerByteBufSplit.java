package zrz.rxjava2.nettybuf;

import java.util.concurrent.Callable;

import com.github.davidmoten.rx2.Callables;
import com.github.davidmoten.rx2.flowable.Transformers;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public class TransformerByteBufSplit {

  private TransformerByteBufSplit() {
    // prevent instantiation
  }

  private static class Transition implements Function3<ByteBuf, ByteBuf, FlowableEmitter<ByteBuf>, ByteBuf> {

    private ByteProcessor pattern;

    public Transition(ByteProcessor pattern) {
      this.pattern = pattern;
    }

    /**
     * perform splits on the buffer using a specified index pattern.
     */

    @Override
    public ByteBuf apply(ByteBuf leftOver, ByteBuf s, FlowableEmitter<ByteBuf> emitter) {

      // need to create a composite buffer for the two.
      if (leftOver != null) {
        s = leftOver.alloc().compositeBuffer(2).addComponents(true, leftOver, s);
      }

      while (s.isReadable()) {

        if (emitter.isCancelled()) {
          // no longer needed, so release.
          s.release();
          return null;
        }

        int nextSplit = s.forEachByte(pattern);

        if (nextSplit == -1) {
          // no match, save for next time. we don't release.
          return s;
        }

        emitter.onNext(s.readRetainedSlice(nextSplit - s.readerIndex() + 1));

      }

      // nothing readable. can release.
      s.release();

      return null;

    }

  }

  private static class Completion implements BiPredicate<ByteBuf, FlowableEmitter<ByteBuf>> {

    @Override
    public boolean test(ByteBuf leftOver, FlowableEmitter<ByteBuf> emitter) {

      if (leftOver != null && !emitter.isCancelled()) {
        emitter.onNext(leftOver);
      }

      if (!emitter.isCancelled()) {
        emitter.onComplete();
      }

      return true;

    }

  }

  public static FlowableTransformer<ByteBuf, ByteBuf> split(ByteProcessor pattern, final BackpressureStrategy backpressureStrategy, int batchSize) {

    Callable<ByteBuf> initialState = Callables.constant(null);

    return Transformers.stateMachine(
        initialState,
        new Transition(pattern),
        new Completion(),
        backpressureStrategy,
        batchSize);

  }

}
