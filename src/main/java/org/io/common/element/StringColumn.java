package org.io.common.element;



import org.io.common.constant.SystemConstants;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */

public class StringColumn extends Column {

	public StringColumn() {
		this((String) null);
	}

	public StringColumn(final String rawData) {
		super(rawData, Type.STRING, (null == rawData ? 0 : rawData
				.length()));
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}

		return (String) this.getRawData();
	}

	private void validateDoubleSpecific(final String data) {
		if ("NaN".equals(data) || "Infinity".equals(data)
				|| "-Infinity".equals(data)) {
			throw new RuntimeException("类型未知");
		}

		return;
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		this.validateDoubleSpecific((String) this.getRawData());

		try {
			return this.asBigDecimal().toBigInteger();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		this.validateDoubleSpecific((String) this.getRawData());

		try {
			BigInteger integer = this.asBigInteger();
			OverFlowUtil.validateLongNotOverFlow(integer);
			return integer.longValue();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public BigDecimal asBigDecimal() {
		if (null == this.getRawData()) {
			return null;
		}

		this.validateDoubleSpecific((String) this.getRawData());

		try {
			return new BigDecimal(this.asString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		String data = (String) this.getRawData();
		if ("NaN".equals(data)) {
			return Double.NaN;
		}

		if ("Infinity".equals(data)) {
			return Double.POSITIVE_INFINITY;
		}

		if ("-Infinity".equals(data)) {
			return Double.NEGATIVE_INFINITY;
		}

		BigDecimal decimal = this.asBigDecimal();
		OverFlowUtil.validateDoubleNotOverFlow(decimal);

		return decimal.doubleValue();
	}

	@Override
	public Boolean asBoolean() {
		if (null == this.getRawData()) {
			return null;
		}

		if ("true".equalsIgnoreCase(this.asString())) {
			return true;
		}

		if ("false".equalsIgnoreCase(this.asString())) {
			return false;
		}
		throw new RuntimeException("类型转换失败");
	}

	@Override
	public Date asDate() {
		if(this.asString() == null){
			return null;
		}
			try {
				return new SimpleDateFormat(SystemConstants.DATE_FORMAT).parse(this.asString());
			}catch (ParseException e){

			}
			try {
				return new SimpleDateFormat(SystemConstants.DATETIME_FORMAT).parse(this.asString());
			}catch (ParseException e){

			}
			try {
				return new SimpleDateFormat(SystemConstants.TIME_FORMAT).parse(this.asString());
			}catch (ParseException e){

			}
			return null;
	}
	
	@Override
	public Date asDate(String dateFormat) {
		Date date =null;
		try {
			date = new SimpleDateFormat(dateFormat).parse(this.asString());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		return date;

	}

	@Override
	public byte[] asBytes() {
		if (null ==this.asString()) {
			return null;
		}
		byte[] bytes=null;
		try {
			bytes = this.asString().getBytes(SystemConstants.ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return bytes;
	}
}
