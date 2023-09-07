package org.io.common.element;

import org.apache.commons.lang3.ArrayUtils;
import org.io.common.constant.SystemConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn extends Column {

	public BytesColumn() {
		this(null);
	}

	public BytesColumn(byte[] bytes) {
		super(ArrayUtils.clone(bytes), Column.Type.BYTES, null == bytes ? 0
				: bytes.length);
	}

	@Override
	public byte[] asBytes() {
		if (null == this.getRawData()) {
			return null;
		}

		return (byte[]) this.getRawData();
	}

	@Override
	public Long asLong() {
		return null;
	}

	@Override
	public BigDecimal asBigDecimal() {
		return null;
	}

	@Override
	public BigInteger asBigInteger() {
		return null;
	}

	@Override
	public Double asDouble() {
		return null;
	}

	@Override
	public Date asDate() {
		return null;
	}

	@Override
	public Date asDate(String dateFormat) {
		return null;
	}

	@Override
	public Boolean asBoolean() {
		return null;
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}
		try {
			return new String((byte[]) this.getRawData(), SystemConstants.ENCODING);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

}
