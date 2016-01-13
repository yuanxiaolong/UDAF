package com.yxl;

import com.yxl.util.JsonFilterUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

/**
 * UDAF 通过条件过滤 执行 wordcount
 */
public class UDAFUuid2TagSum extends AbstractGenericUDAFResolver {

    private static final Log LOG = LogFactory
            .getLog(UDAFUuid2TagSum.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        return new GenericUdafSumEvaluator();
    }

    public static class GenericUdafSumEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector inputOI2;
        private PrimitiveObjectInspector inputOI3;
        private PrimitiveObjectInspector outputOI;
        private IntWritable result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            //init input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){ //必须得有
                LOG.info(" Mode:"+m.toString()+" result has init");
                inputOI = (PrimitiveObjectInspector) parameters[0];
                inputOI2 = (PrimitiveObjectInspector) parameters[1];
                if (parameters.length == 3){
                    inputOI3 = (PrimitiveObjectInspector) parameters[2];
                }
            }
            //init output
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                outputOI = (PrimitiveObjectInspector) parameters[0];
                result = new IntWritable(0);
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            }else{
                result = new IntWritable(0);
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            }

        }

        /** class for storing count value. */
        static class SumAgg implements AggregationBuffer {
            boolean empty;
            int value;
        }

        @Override
        //创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
        //使用buffer对象前，先进行内存的清空——reset
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumAgg buffer = new SumAgg();
            reset(buffer);
            return buffer;
        }

        @Override
        //重置为0
        //mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
        public void reset(AggregationBuffer agg) throws HiveException {
            ((SumAgg) agg).value = 0;
            ((SumAgg) agg).empty = true;
        }

        //迭代
        //只要把保存当前和的对象agg，再加上输入的参数，就可以了。
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            // parameters == null means the input table/split is empty
            if (parameters == null) {
                return;
            }
            try {
                int tagid = PrimitiveObjectInspectorUtils.getInt(parameters[1], inputOI2);
                String tag = null;
                if (parameters.length ==3){
                    tag = PrimitiveObjectInspectorUtils.getString(parameters[2], inputOI3);
                }
                int matchRet = JsonFilterUtil.match(PrimitiveObjectInspectorUtils.getString(parameters[0], inputOI), tagid, tag);
                //这里将迭代数据放入combiner进行合并
                merge(agg,matchRet);
            } catch (Exception e) {
                LOG.error("iter happend error: ",e);
            }

        }

        @Override
        //这里的操作就是具体的聚合操作。
        public void merge(AggregationBuffer agg, Object partial) {
            if (partial != null) {
                // 通过ObejctInspector取每一个字段的数据
                if (inputOI != null) {
                    ((SumAgg) agg).value += (Integer)partial;
                } else {
                    int p = PrimitiveObjectInspectorUtils.getInt(partial,
                            outputOI);
                    ((SumAgg) agg).value += p;
                }
            }
        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer agg){
            SumAgg myagg = (SumAgg) agg;
            result.set(myagg.value);
            return result;
        }
    }
}
