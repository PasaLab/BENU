package cn.edu.nju.pasalab.db.util;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by wzk on 18-4-28.
 */
public final class CoderAndDecoder {

        public static byte[] toByteArray(long key) {
            byte result[] = new byte[8];
            for (int i = 0; i < 8; i++) {
                result[i] = (byte)key;
                key >>= 8;
            }
            return result;
        }

        public static byte[] toByteArray(long array[]) throws IOException {
            return Snappy.compress(array);
        }

        public static long toLong(byte data[]) {
            long result = 0L;
            for (int i = 7; i >= 0; i--) {
                result <<= 8;
                result |= ((long)data[i] & 0x0FFL);
            }
            return result;
        }

        public static long[] toLongArray(byte data[]) throws IOException {
            return Snappy.uncompressLongArray(data);
        }

}
