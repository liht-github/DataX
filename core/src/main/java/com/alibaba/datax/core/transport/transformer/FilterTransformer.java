package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * 过滤FilterTransformer
 * Created by liqiang on 16/3/4.
 */
public class FilterTransformer extends Transformer {
    public FilterTransformer() {
        setTransformerName("dx_filter");
    }

    /**
     * 参数： <br>
     * 第一个参数：字段编号，对应record中第几个字段。 <br>
     * 第二个参数：运算符，支持一下运算符：like, not like, >, =, <, >=, !=, <= <br>
     * 第三个参数：正则表达式（java正则表达式）、值。 <br>
     * 返回：
     * 如果匹配正则表达式，返回Null，表示过滤该行。不匹配表达式时，表示保留该行。（注意是该行）。对于>=<都是对字段直接compare的结果. <br>
     * like ， not like是将字段转换成String，然后和目标正则表达式进行全匹配。 <br>
     * , =, <, >=, !=, <= 对于DoubleColumn比较double值，对于LongColumn和DateColumn比较long值， <br>
     * 其他StringColumn，BooleanColumn以及ByteColumn均比较的是StringColumn值。 <br>
     * <p>
     * 如果目标column为空（null），对于 = null的过滤条件，将满足条件，被过滤。！=null的过滤条件，null不满足过滤条件，不被过滤。 <br>
     * like，字段为null不满足条件，不被过滤，和not like，字段为null满足条件，被过滤。 <br>
     * 举例： <br>
     * dx_filter(1,"like","dataTest") <br>
     * dx_filter(1,">=","10") <br>
     *
     * @param record Record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  Object transformer函数参数
     * @return Record
     */
    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String code;
        String value;

        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_filter paras must be 3");
            }

            columnIndex = (Integer) paras[0];
            code = (String) paras[1];
            value = (String) paras[2];

            if (StringUtils.isEmpty(value)) {
                throw new RuntimeException("dx_filter para 2 can't be null");
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }


        Column column = record.getColumn(columnIndex);

        try {

            if (code.equalsIgnoreCase("like")) {
                return doLike(record, value, column);
            } else if (code.equalsIgnoreCase("not like")) {
                return doNotLike(record, value, column);
            } else if (code.equalsIgnoreCase(">")) {
                return doGreat(record, value, column, false);
            } else if (code.equalsIgnoreCase("<")) {
                return doLess(record, value, column, false);
            } else if (code.equalsIgnoreCase("=") || code.equalsIgnoreCase("==")) {
                return doEqual(record, value, column);
            } else if (code.equalsIgnoreCase("!=")) {
                return doNotEqual(record, value, column);
            } else if (code.equalsIgnoreCase(">=")) {
                return doGreat(record, value, column, true);
            } else if (code.equalsIgnoreCase("<=")) {
                return doLess(record, value, column, true);
            } else {
                throw new RuntimeException("dx_filter can't support code:" + code);
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
    }


    private Record doGreat(Record record, String value, Column column, boolean hasEqual) {

        //如果字段为空，直接不参与比较。即空也属于无穷小
        if (column.getRawData() == null) {
            return record;
        }
        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (hasEqual) {
                if (ori >= val) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori > val) {
                    return null;
                } else {
                    return record;
                }
            }
        } else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (hasEqual) {
                if (ori >= val) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori > val) {
                    return null;
                } else {
                    return record;
                }
            }
        } else if (column instanceof StringColumn || column instanceof BytesColumn || column instanceof BoolColumn) {
            String ori = column.asString();
            if (hasEqual) {
                if (ori.compareTo(value) >= 0) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori.compareTo(value) > 0) {
                    return null;
                } else {
                    return record;
                }
            }
        } else {
            throw new RuntimeException(">=,> can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    private Record doLess(Record record, String value, Column column, boolean hasEqual) {

        //如果字段为空，直接不参与比较。即空也属于无穷大
        if (column.getRawData() == null) {
            return record;
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (hasEqual) {
                if (ori <= val) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori < val) {
                    return null;
                } else {
                    return record;
                }
            }
        } else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (hasEqual) {
                if (ori <= val) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori < val) {
                    return null;
                } else {
                    return record;
                }
            }
        } else if (column instanceof StringColumn || column instanceof BytesColumn || column instanceof BoolColumn) {
            String ori = column.asString();
            if (hasEqual) {
                if (ori.compareTo(value) <= 0) {
                    return null;
                } else {
                    return record;
                }
            } else {
                if (ori.compareTo(value) < 0) {
                    return null;
                } else {
                    return record;
                }
            }
        } else {
            throw new RuntimeException("<=,< can't support this columnType:" + column.getClass().getSimpleName());
        }

    }

    /**
     * DateColumn将比较long值，StringColumn，ByteColumn以及BooleanColumn比较其String值
     *
     * @param record
     * @param value
     * @param column
     * @return 如果相等，则过滤。
     */

    private Record doEqual(Record record, String value, Column column) {

        //如果字段为空，只比较目标字段为"null"，否则null字段均不过滤
        if (column.getRawData() == null) {
            if (value.equalsIgnoreCase("null")) {
                return null;
            } else {
                return record;
            }
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (ori == val) {
                return null;
            } else {
                return record;
            }
        } else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (ori == val) {
                return null;
            } else {
                return record;
            }
        } else if (column instanceof StringColumn || column instanceof BytesColumn || column instanceof BoolColumn) {
            String ori = column.asString();
            if (ori.compareTo(value) == 0) {
                return null;
            } else {
                return record;
            }
        } else {
            throw new RuntimeException("== can't support this columnType:" + column.getClass().getSimpleName());
        }

    }

    /**
     * DateColumn将比较long值，StringColumn，ByteColumn以及BooleanColumn比较其String值
     *
     * @param record
     * @param value
     * @param column
     * @return 如果不相等，则过滤。
     */
    private Record doNotEqual(Record record, String value, Column column) {

        //如果字段为空，只比较目标字段为"null", 否则null字段均过滤。
        if (column.getRawData() == null) {
            if (value.equalsIgnoreCase("null")) {
                return record;
            } else {
                return null;
            }
        }

        if (column instanceof DoubleColumn) {
            Double ori = column.asDouble();
            double val = Double.parseDouble(value);

            if (ori != val) {
                return null;
            } else {
                return record;
            }
        } else if (column instanceof LongColumn || column instanceof DateColumn) {
            Long ori = column.asLong();
            long val = Long.parseLong(value);

            if (ori != val) {
                return null;
            } else {
                return record;
            }
        } else if (column instanceof StringColumn || column instanceof BytesColumn || column instanceof BoolColumn) {
            String ori = column.asString();
            if (ori.compareTo(value) != 0) {
                return null;
            } else {
                return record;
            }
        } else {
            throw new RuntimeException("== can't support this columnType:" + column.getClass().getSimpleName());
        }
    }

    private Record doLike(Record record, String value, Column column) {
        String orivalue = column.asString();
        if (orivalue != null && orivalue.matches(value)) {
            return null;
        } else {
            return record;
        }
    }

    private Record doNotLike(Record record, String value, Column column) {
        String orivalue = column.asString();
        if (orivalue != null && orivalue.matches(value)) {
            return record;
        } else {
            return null;
        }
    }
}
