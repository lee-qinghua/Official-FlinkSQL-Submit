package com.otis.flink.udaf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import java.util.LinkedList;
import java.util.Queue;

public abstract class PreviousValueAggFunction<T> extends AggregateFunction<T, PreviousValueAggFunction.PreviousValue<T>> {
    public static class PreviousValue<T> {
        public Queue<T> queue;

        public int length;

        public Object defaultValue;
    }

    public boolean isDeterministic() {
        return true;
    }

    public PreviousValue<T> createAccumulator() {
        PreviousValue<T> acc = new PreviousValue<>();
        acc.queue = new LinkedList<>();
        acc.length = 0;
        acc.defaultValue = null;
        return acc;
    }

    public void accumulate(PreviousValue<T> acc, Object value, int length, Object defaultValue) {
        acc.length = length;
        acc.defaultValue = defaultValue;
        int accSize = acc.queue.size();
        if (accSize < length)
            for (int i = 0; i < length - accSize; i++)
                acc.queue.offer((T)defaultValue);
        if (value != null) {
            acc.queue.offer((T)value);
        } else {
            acc.queue.offer((T)defaultValue);
        }
    }

    public void accumulate(PreviousValue<T> acc, Object value, int length) {
        acc.length = length;
        acc.defaultValue = value;
        int accSize = acc.queue.size();
        if (accSize < length)
            for (int i = 0; i < length - accSize; i++)
                acc.queue.offer((T)value);
        acc.queue.offer((T)value);
    }

    public void resetAccumulator(PreviousValue<Object> acc) {
        acc.length = 0;
        acc.defaultValue = null;
        acc.queue.clear();
    }

    public T getValue(PreviousValue<T> acc) {
        return acc.queue.poll();
    }

    public static class BytePreviousValueAggFunction extends PreviousValueAggFunction<Byte> {
        public TypeInformation<Byte> getResultType() {
            return Types.BYTE;
        }
    }

    public static class LongPreviousValueAggFunction extends PreviousValueAggFunction<Long> {
        public TypeInformation<Long> getResultType() {
            return Types.LONG;
        }
    }

    public static class ShortPreviousValueAggFunction extends PreviousValueAggFunction<Short> {
        public TypeInformation<Short> getResultType() {
            return Types.SHORT;
        }
    }

    public static class IntPreviousValueAggFunction extends PreviousValueAggFunction<Integer> {
        public TypeInformation<Integer> getResultType() {
            return Types.INT;
        }
    }

    public static class FloatPreviousValueAggFunction extends PreviousValueAggFunction<Float> {
        public TypeInformation<Float> getResultType() {
            return Types.FLOAT;
        }
    }

    public static class DoublePreviousValueAggFunction extends PreviousValueAggFunction<Double> {
        public TypeInformation<Double> getResultType() {
            return Types.DOUBLE;
        }
    }

    public static class BooleanPreviousValueAggFunction extends PreviousValueAggFunction<Boolean> {
        public TypeInformation<Boolean> getResultType() {
            return Types.BOOLEAN;
        }
    }

    public static class DecimalPreviousValueAggFunction extends PreviousValueAggFunction<Decimal> {
        private DecimalTypeInfo decimalTypeInfo;

        public DecimalPreviousValueAggFunction(DecimalTypeInfo decimalTypeInfo) {
            this.decimalTypeInfo = decimalTypeInfo;
        }

        public DecimalPreviousValueAggFunction() {
            this.decimalTypeInfo = new DecimalTypeInfo(20, 6);
        }

        public void accumulate(PreviousValue<Decimal> acc, Decimal value, int length, Object defaultValue) {
            accumulate((PreviousValue)acc, value, length, defaultValue);
        }

        public void accumulate(PreviousValue<Decimal> acc, Decimal value, int length) {
            accumulate((PreviousValue)acc, value, length);
        }

        public TypeInformation<Decimal> getResultType() {
            return (TypeInformation<Decimal>)this.decimalTypeInfo;
        }
    }

    public static class StringPreviousValueAggFunction extends PreviousValueAggFunction<BinaryString> {
        public TypeInformation<BinaryString> getResultType() {
            return (TypeInformation<BinaryString>)BinaryStringTypeInfo.INSTANCE;
        }

        public void accumulate(PreviousValue<BinaryString> acc, BinaryString value, int length, Object defaultValue) {
            if (value != null) {
                accumulate((PreviousValue)acc, value.copy(), length, defaultValue);
            } else {
                accumulate((PreviousValue)acc, null, length, defaultValue);
            }
        }

        public void accumulate(PreviousValue<BinaryString> acc, BinaryString value, int length) {
            if (value != null) {
                accumulate((PreviousValue)acc, value.copy(), length);
            } else {
                accumulate((PreviousValue)acc, null, length);
            }
        }
    }
}
