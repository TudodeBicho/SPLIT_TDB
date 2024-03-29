
package br.com.sattva.tdb.utils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.EntityDAO;
import br.com.sankhya.jape.dao.EntityPropertyDescriptor;
import br.com.sankhya.jape.dao.PersistentObjectUID;
import br.com.sankhya.jape.metadata.EntityField;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.mgecomercial.model.facades.helpper.DuplicacaoNotaHelper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.comercial.LiberacaoSolicitada;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sankhya.util.TimeUtils;

public class CentralNotasUtils {

	public static BigDecimal duplicaWithTx(final BigDecimal nuNota, JapeSession.SessionHandle hnd) throws Exception {
		final BigDecimal[] nuNotaDest = new BigDecimal[1];
		hnd.execWithTX(new JapeSession.TXBlock() {
			@Override
			public void doWithTx() throws Exception {
				nuNotaDest[0] = duplica(nuNota);
			}
		});
		return nuNotaDest[0];

	}

	public static BigDecimal duplica(BigDecimal nuNota) throws Exception {
		JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
		JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
		JapeSession.putProperty("ItemNota.incluindo.alterando.pela.central", Boolean.TRUE);
		JapeWrapper origemDAO = JapeFactory.dao("CabecalhoNota");
		DynamicVO origemVO = origemDAO.findByPK(nuNota);
		DuplicacaoNotaHelper.DuplicacaoParams p = new DuplicacaoNotaHelper.DuplicacaoParams();
		p.dataSaida = null;
		p.serie = (String) origemVO.asString("SERIENOTA");
		p.atualizaPreco = false;
		p.codigoTopDestino = origemVO.asBigDecimal("CODTIPOPER");
		p.duplicarTodosItens = false;

		Map itensPorNota = new HashMap();

		itensPorNota.put(origemVO.asBigDecimal("NUNOTA"), null);
		DuplicacaoNotaHelper helper = DuplicacaoNotaHelper.buildHelper(p, itensPorNota);

		List<DuplicacaoNotaHelper.Duplicacao> duplicacaos = helper.duplicarNotas();
		return duplicacaos.get(0).numeroNotaDestino;
	}

	public static Map<String, Object> duplicaRegistro(DynamicVO voOrigem, String entidade, Map<String, Object> map)
			throws Exception {
		EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
		EntityDAO rootDAO = dwfFacade.getDAOInstance(entidade);
		DynamicVO destinoVO = voOrigem.buildClone();
		limparPk(destinoVO, rootDAO);

		for (String campo : map.keySet()) {

			destinoVO.setProperty(campo, map.get(campo));
		}
		PersistentLocalEntity createEntity = dwfFacade.createEntity(entidade, (EntityVO) destinoVO);
		DynamicVO save = (DynamicVO) createEntity.getValueObject();

		return buildPk(save, rootDAO);
	}

	public static Map<String, Object> buildPk(DynamicVO vo, EntityDAO rootDAO) throws Exception {

		PersistentObjectUID objectUID = rootDAO.getSQLProvider().getPkObjectUID();
		EntityPropertyDescriptor[] pkFields = objectUID.getFieldDescriptors();
		Map<String, Object> pkArray = new HashMap<>();
		for (int i = 0; i < pkFields.length; i++) {
			EntityField field = pkFields[i].getField();

			String fieldName = field.getName();
			Object typedValue = vo.getProperty(fieldName);

			if (typedValue == null) {
				return null;
			}

			pkArray.put(fieldName, typedValue);
		}

		return pkArray;
	}

	public static void limparPk(DynamicVO vo, EntityDAO rootDAO) throws Exception {
		PersistentObjectUID objectUID = rootDAO.getSQLProvider().getPkObjectUID();
		EntityPropertyDescriptor[] pkFields = objectUID.getFieldDescriptors();

		for (EntityPropertyDescriptor pkField : pkFields) {
			vo.setProperty(pkField.getField().getName(), null);
		}

	}

	public static boolean confirmarNota(BigDecimal nuNota) throws Exception {
		CACHelper cacHelper = new CACHelper();
		BarramentoRegra barramento = BarramentoRegra.build(CACHelper.class, "regrasConfirmacaoCAC.xml",
				AuthenticationInfo.getCurrent());

		cacHelper.confirmarNota(nuNota, barramento, false);

		StringBuilder toResult = new StringBuilder();

		if (barramento.getLiberacoesSolicitadas().size() == 0 && barramento.getErros().size() == 0) {
			return true;
		} else {

			if (barramento.getErros().size() > 0) {

				for (Exception e : barramento.getErros()) {
					toResult.append(e.getMessage());
				}

				throw new Exception(toResult.toString());
			}

			if (barramento.getLiberacoesSolicitadas().size() > 0) {
				toResult.append("Libera��es solicitadas - \n");

				for (LiberacaoSolicitada e : barramento.getLiberacoesSolicitadas()) {
					toResult.append("Evento: ").append(e.getEvento())
							.append(e.getDescricao() != null ? " Descri��o:  " + e.getDescricao() + "\n" : "\n");
					break;
				}

			}

			JapeWrapper filaDAO = JapeFactory.dao("MSDFilaMensagem");
			DynamicVO save = filaDAO.create().set("DTENTRADA", TimeUtils.getNow()).set("STATUS", "Pendente")
					.set("CODCON", BigDecimal.ZERO).set("MENSAGEM", toResult).set("TIPOENVIO", "E")
					.set("MAXTENTENVIO", new BigDecimal("3"))
					.set("ASSUNTO", "Valida��es da Rotina de Confirma��o de Requisi��es e Transferencias")
					.set("EMAIL", "bruno.campos@sankhya.com.br").save();

		}
		return false;
	}

	public static void confirmarNota(BigDecimal nuNota, boolean refazerFinanceiro) throws Exception {
		if (refazerFinanceiro)
			refazerFinanceiro(nuNota);
		confirmarNota(nuNota);
	}

	public static void refazerFinanceiro(BigDecimal nuNota) throws Exception {

		JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
		JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
		JapeSessionContext.putProperty("ItemNota.incluindo.alterando.pela.central", Boolean.TRUE);
		JapeSessionContext.putProperty("calcular.outros.impostos", "false");
		CentralFinanceiro financeiro = new CentralFinanceiro();
		financeiro.inicializaNota(nuNota);
		financeiro.refazerFinanceiro();
	}

}