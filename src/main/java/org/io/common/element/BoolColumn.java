package org.io.common.element;



import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn extends Column {

    public BoolColumn(Boolean bool) {
        super(bool, Column.Type.BOOL, 1);
    }

    public BoolColumn(final String data) {
        this(true);
        this.validate(data);
        if (null == data) {
            this.setRawData(null);
            this.setByteSize(0);
        } else {
            this.setRawData(Boolean.valueOf(data));
            this.setByteSize(1);
        }
        return;
    }

    public BoolColumn() {
        super(null, Column.Type.BOOL, 1);
    }


    public Boolean asBoolean() {
        if (null == super.getRawData()) {
            return null;
        }

        return (Boolean) super.getRawData();
    }


    public Long asLong() {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBoolean() ? 1L : 0L;
    }


    public Double asDouble() {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBoolean() ? 1.0d : 0.0d;
    }

    public String asString() {
        if (null == super.getRawData()) {
            return null;
        }

        return this.asBoolean() ? "true" : "false";
    }


    public BigInteger asBigInteger() {
        if (null == this.getRawData()) {
            return null;
        }

        return BigInteger.valueOf(this.asLong());
    }


    public BigDecimal asBigDecimal() {
        if (null == this.getRawData()) {
            return null;
        }

        return BigDecimal.valueOf(this.asLong());
    }


    public Date asDate() {
        throw new RuntimeException("Bool类型不能转为Date .");
    }


    public Date asDate(String dateFormat) {
       return null;
    }


    public byte[] asBytes() {
        return null;
    }

    private void validate(final String data) {
        if (null == data) {
            return;
        }

        if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) {
            return;
        }
        throw new RuntimeException(String.format("String[%s]不能转为Bool .", data));
    }
}
