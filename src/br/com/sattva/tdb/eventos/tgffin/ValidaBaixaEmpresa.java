package br.com.sattva.tdb.eventos.tgffin;

import java.math.BigDecimal;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;

public class ValidaBaixaEmpresa implements EventoProgramavelJava {

	@Override
	public void afterDelete(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void afterInsert(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void afterUpdate(PersistenceEvent arg0) throws Exception {}

	@Override
	public void beforeCommit(TransactionContext arg0) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeDelete(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeInsert(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeUpdate(PersistenceEvent arg0) throws Exception {
		DynamicVO finVO = (DynamicVO) arg0.getVo();
		DynamicVO finOldVO = (DynamicVO) arg0.getOldVO();
		
		if (finVO.asTimestamp("DHBAIXA") != null && finOldVO.asTimestamp("DHBAIXA") == null) {
			BigDecimal contaBaixa = finVO.asBigDecimal("CODCTABCOINT");
			if (contaBaixa != null) {
				BigDecimal empresaBaixa = finVO.asBigDecimal("CODEMPBAIXA");
				JapeWrapper contaDAO = JapeFactory.dao("ContaBancaria");
				DynamicVO contaVO = contaDAO.findOne("CODCTABCOINT = ?", contaBaixa);
				
				BigDecimal empresaConta = contaVO.asBigDecimal("CODEMP");	
				
				if (empresaBaixa.compareTo(empresaConta) != 0) {
					
					JapeWrapper empresaDAO = JapeFactory.dao("Empresa");
					DynamicVO empresaVO = empresaDAO.findOne("CODEMP = ?", empresaBaixa);
					
					BigDecimal empresaMatriz = empresaVO.asBigDecimal("CODEMPMATRIZ");
					
					if (empresaMatriz == null) {
						throw new Exception("\n<b>A conta utilizada na baixa não pertence à essa empresa. Favor verificar!</b>\n");
					}
					
					if (empresaMatriz.compareTo(empresaConta) != 0) {
						throw new Exception("\n<b>A conta utilizada na baixa não pertence à essa empresa. Favor verificar!</b>\n");
					}
				}
			}
		}
	}
}
