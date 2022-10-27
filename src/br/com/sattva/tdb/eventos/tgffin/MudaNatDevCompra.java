package br.com.sattva.tdb.eventos.tgffin;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.motazan.model.dao.MGEParameters;

public class MudaNatDevCompra implements EventoProgramavelJava {

	@Override
	public void afterDelete(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void afterInsert(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void afterUpdate(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

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
		DynamicVO financeiroVO = (DynamicVO) arg0.getVo();
		BigDecimal codTipOper = financeiroVO.asBigDecimal("CODTIPOPER");
		Timestamp dhTipOper = financeiroVO.asTimestamp("DHTIPOPER");
		String origem = financeiroVO.asString("ORIGEM");

		if (origem.equals("E")) {
			BigDecimal nuNota = financeiroVO.asBigDecimal("NUNOTA");
			
			if (codTipOper == null) {
				return;
			}

			JapeWrapper topDAO = JapeFactory.dao(DynamicEntityNames.TIPO_OPERACAO);
			DynamicVO topVO = topDAO.findOne("CODTIPOPER = ? AND DHALTER = ?", codTipOper, dhTipOper);
			
			if (topVO == null) {
				return;
			}
			
			if (!"E".equals(topVO.asString("TIPMOV"))) {
				return;
			}
			
			if (topVO.asString("AD_USANATPADGLOBDEV") == null) {
				return;
			}

			if (topVO.asString("AD_USANATPADGLOBDEV").equals("S")) {
				JapeWrapper paramDAO = JapeFactory.dao("ParametroSistema");
				DynamicVO paramVO = paramDAO.findOne("CHAVE = 'NATPADDEVCPA'");
				BigDecimal newCodNat = paramVO.asBigDecimal("INTEIRO");

				financeiroVO.setProperty("CODNAT", newCodNat);

				JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
				cabDAO.prepareToUpdateByPK(nuNota).set("CODNAT", newCodNat).update();

			}
		}
	}

	@Override
	public void beforeUpdate(PersistenceEvent arg0) throws Exception {
		// TODO Auto-generated method stub
	}

}
