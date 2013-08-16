package com.nexr.platform.hive.udf;

import java.util.List;
import java.util.ArrayList;
import java.lang.NumberFormatException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

@Description(name="array_string_to_double", value="_FUNC_(array<type>) - Converts array of strings to doubles")
public class UDFArrayStringtoDouble extends UDF {
    ListObjectInspector arrayInspector;
    ListObjectInspector elementsInspector;

    public List<Double> evaluate(List<String> a) {
		List<Double> ret = new ArrayList<Double>();
        if (a == null) return null;

		for (int i = 0; i < a.size(); i++) {
			try {
				Double n = Double.valueOf(a.get(i).trim());
				if (n != null) {
					ret.add(n);
				}
			} catch (NumberFormatException e) {
			}
		}
        return ret;
    }
}

