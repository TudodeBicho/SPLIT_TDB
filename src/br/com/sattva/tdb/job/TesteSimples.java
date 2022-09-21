package br.com.sattva.tdb.job;

import java.math.BigDecimal;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;

public class TesteSimples implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {
		System.out.println("Teste Simples ação");
		JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
		try {
			DynamicVO cabVO = cabDAO.findOne("NUNOTA = ?", new BigDecimal("2462697"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
