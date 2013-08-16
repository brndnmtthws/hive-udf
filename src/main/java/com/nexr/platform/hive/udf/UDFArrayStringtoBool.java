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

@Description(name="array_string_to_bool", value="_FUNC_(array<type>) - Converts array of strings to booleans")
public class UDFArrayStringtoBool extends UDF {
    ListObjectInspector arrayInspector;
    ListObjectInspector elementsInspector;

    public List<Boolean> evaluate(List<String> a) {
		List<Boolean> ret = new ArrayList<Boolean>();
        if (a == null) return null;

		for (int i = 0; i < a.size(); i++) {
			String s = a.get(i).trim().toLowerCase();
			if (s.equals("true") || s.equals("yes")) {
				ret.add(Boolean.valueOf(true));
			} else if (s.equals("false") || s.equals("no")) {
				ret.add(Boolean.valueOf(false));
			}
		}
        return ret;
    }
}

