package br.com.sattva.tdb.callback;

import java.util.Map;

import br.com.sankhya.modelcore.custommodule.ICustomCallBack;

public class CallBackFaturamento implements ICustomCallBack {

	@Override
	public Object call(String arg0, Map<String, Object> arg1) {
		// TODO Auto-generated method stub
		
		System.out.println("[Sattva] - arg0: " + arg0);
		System.out.println("[Sattva] - arg0: " + arg0);

		for (String key : arg1.keySet()) {

            //Capturamos o valor a partir da chave
            Object value = arg1.get(key);
            System.out.println("[Sattva] - arg1: " + key + " = " + value);
     }
		
		return null;
	}

}
 