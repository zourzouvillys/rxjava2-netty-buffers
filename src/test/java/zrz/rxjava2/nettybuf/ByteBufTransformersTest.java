package zrz.rxjava2.nettybuf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import io.reactivex.Flowable;
import zrz.rxjava2.nettybuf.ByteBufTransformers;
import zrz.rxjava2.nettybuf.ByteBufs;

public class ByteBufTransformersTest {

  @Test
  public void test() {

    ByteBuf testBuf = Unpooled.copiedBuffer("hello there\n1234\ntesting\ntrailing", UTF_8);

    List<String> res = Flowable.just(testBuf)
        .compose(ByteBufTransformers.split(ByteProcessor.FIND_LF))
        .map(ByteBufs.releasedMap(buf -> buf.toString(UTF_8)))
        .toList()
        .blockingGet();

    // match?
    assertThat(res, CoreMatchers.equalTo(Arrays.asList("hello there\n", "1234\n", "testing\n", "trailing")));

    // make sure we released.
    assertEquals(0, testBuf.refCnt());

  }

}
