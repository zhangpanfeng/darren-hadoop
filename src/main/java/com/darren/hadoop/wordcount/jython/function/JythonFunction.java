package com.darren.hadoop.wordcount.jython.function;

import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class JythonFunction {
    private PythonInterpreter pythonInterpreter;

    public JythonFunction() {
        this.pythonInterpreter = new PythonInterpreter();
    }

    static {
        System.setProperty("python.console.encoding", "utf-8");
        System.setProperty("python.import.site", "false");
        System.setProperty("python.home", "C:/jython2.7.0");
    }

    public void callPython(String filePath, String functionName) {
        this.pythonInterpreter.execfile(filePath);
        
        PyFunction pyFunction = this.pythonInterpreter.get(functionName, PyFunction.class);
        PyObject dddRes = pyFunction.__call__(Py.newInteger(2), Py.newInteger(3));
        
        System.out.println(dddRes);
        this.pythonInterpreter.cleanup();
        this.pythonInterpreter.close();
    }
}
