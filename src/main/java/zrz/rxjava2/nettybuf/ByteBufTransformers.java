package zrz.rxjava2.nettybuf;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableTransformer;

public class ByteBufTransformers {

  public static FlowableTransformer<ByteBuf, ByteBuf> split(ByteProcessor pattern) {
    return split(pattern, BackpressureStrategy.BUFFER, 128);
  }

  public static FlowableTransformer<ByteBuf, ByteBuf> split(ByteProcessor pattern, BackpressureStrategy backpressureStrategy, int requestBatchSize) {
    return TransformerByteBufSplit.split(pattern, backpressureStrategy, requestBatchSize);
  }

}
