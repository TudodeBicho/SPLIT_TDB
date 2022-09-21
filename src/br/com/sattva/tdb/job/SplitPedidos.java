package br.com.sattva.tdb.job;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.FinderWrapper;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;
import br.com.sattva.tdb.utils.TdbHelper;

public class SplitPedidos implements ScheduledAction {
	public String log = "";
	public String regraPrioridade = "";
	public String statusProcessamento = "S";
	public String msgStatus = "";
	boolean geraTransferencia = false;
	final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
	final BigDecimal empresa1 = new BigDecimal("1"); // TDB
	final BigDecimal empresa6 = new BigDecimal("6"); // Loja
	JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
	JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
	JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
	EntityFacade dwf = EntityFacadeFactory.getDWFFacade();

	@Override
	public void onTime(ScheduledActionContext arg0) {
		try {
			System.out.println("=============================== iniciando job split ===============================");
			
			BigDecimal topPreSplit = TdbHelper.buscaTopPreSplit();
			
			NativeSql sqlPedidosAptos = new NativeSql(dwf.getJdbcWrapper());
			sqlPedidosAptos.loadSql(getClass(), "qryPedidosAptos.sql");
			sqlPedidosAptos.setNamedParameter("CODTIPOPER", topPreSplit);
			
			System.out.println("[Sattva] - 2");

			ResultSet rs = sqlPedidosAptos.executeQuery();
			while (rs.next()) {
				
				PersistentLocalEntity registroPLE = dwf.findEntityByPrimaryKey("CabecalhoNota", rs.getBigDecimal("NUNOTA"));
				DynamicVO pedidoVO = (DynamicVO) registroPLE.getValueObject();
				
				log = "";
				System.out.println("[Sattva] - 2.1");
				regraPrioridade = "";
				geraTransferencia = false;
				BigDecimal nuNotaPreSplit = pedidoVO.asBigDecimal("NUNOTA");
				System.out.println("[Sattva] - 2.2");
				BigDecimal codCidadeDestino = pedidoVO.asBigDecimal("CODCIDDESTINO");
				System.out.println("[Sattva] - 2.3");
				String nomeCidadeDestino = buscaCidadeDestino(codCidadeDestino);
				Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
				Collection<Split> splitPedidos = new ArrayList<Split>();

				boolean pedidoJaFaturado = verificaPedidoFaturado(nuNotaPreSplit);
				
				System.out.println("[Sattva] - 3");
				
				if (pedidoJaFaturado) {
					log = "Esse pedido ja foi faturado. Verificar documentos relacionados";
					statusProcessamento = "E";
					logDAO.create().set("DESCRICAO", log.toCharArray())
							.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
							.set("DHINCLUSAO", TimeUtils.getNow())
							.set("STATUSPROCESSAMENTO", statusProcessamento)
							.save();

					continue;
				}

				System.out.println("[Sattva] - Nro Unico do Pre Split: " + nuNotaPreSplit);

				if (codCidadeDestino != null && "4798-2475".indexOf(codCidadeDestino.toString()) > -1) {
					log += "Regra Prioridade: Empresa 1, Empresa 6 e Empresa 5";
					regraPrioridade = "165";
				} else {
					log += "Regra Prioridade: Empresa 5, Empresa 1 e Empresa 6";
					regraPrioridade = "516";
				}

				System.out.println("[Sattva] - 4");
				
				FinderWrapper finderItensPre = new FinderWrapper("ItemNota", "this.NUNOTA = ?", new Object[] {nuNotaPreSplit});
				Collection<PersistentLocalEntity> colletionItensPrePLE = dwf.findByDynamicFinder(finderItensPre);
				
				Collection<DynamicVO> itensPreSplit = new ArrayList<DynamicVO>();
				for (PersistentLocalEntity itemPrePLE : colletionItensPrePLE) {
					itensPreSplit.add((DynamicVO)itemPrePLE.getValueObject());
				}
				
				
				
//				Collection<DynamicVO> itensPreSplit = itemDAO.find("NUNOTA = ?", nuNotaPreSplit);
				
				log += "\nNroUnico Pre Split: " + nuNotaPreSplit;
				log += "\nQuantidade de Itens: " + itensPreSplit.size();
				log += "\nCidade de Destino:" + codCidadeDestino + "-" + nomeCidadeDestino;
				System.out.println("[Sattva] - Chegou aqui...");

				System.out.println("Antes");
				doSplit(regraPrioridade, itensPreSplit, nuNotaPreSplit, itensTransferencia, splitPedidos);
				System.out.println("Depois do doSplit");

				if (geraTransferencia) {
					Map<String, BigDecimal> nroUnicoTransf = TdbHelper.transfereSaldo6x1(itensTransferencia);
					log += "\n\nNro.Unico.Transferencia Saida..: " + nroUnicoTransf.get("NUNOTATRANSFSAIDA");
					log += "\nNro.Unico.Transferencia Entrada: " + nroUnicoTransf.get("NUNOTATRANSFENTRADA") + "\n";

					System.out.println("[Sattva] - Recalculando transferencia");
					TdbHelper.recalculaImpostoEFinanceiro(nroUnicoTransf.get("NUNOTATRANSFSAIDA"));
					TdbHelper.recalculaImpostoEFinanceiro(nroUnicoTransf.get("NUNOTATRANSFENTRADA"));
					System.out.println("[Sattva] - Fim - Recalculando transferencia");
				}

				System.out.println("Depois da transferencia");
				Collection<Split> pedidosSplit = TdbHelper.agrupaSplitPorEmpresa(splitPedidos);

				Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa = new HashMap<BigDecimal, BigDecimal>();
				
				authenticate(BigDecimal.ZERO);

				System.out.println("GeraLancamentosSplit");
				listaNroUnicoEmpresa = TdbHelper.geraLancamentosSplit(pedidoVO, pedidosSplit);

				imprimeSplitFinal(pedidosSplit, listaNroUnicoEmpresa);

				BigDecimal topPosSplit = TdbHelper.buscaTopPosSplit();

				cabecalhoDAO.prepareToUpdate(pedidoVO).set("CODTIPOPER", topPosSplit)
						.set("DHTIPOPER", TdbHelper.getDhTipOper(topPosSplit)).update();

//                    	CentralNotasUtils.confirmarNota(pedidoVO.asBigDecimal("NUNOTA"));

				logDAO.create().set("DESCRICAO", log.toCharArray()).set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
						.set("DHINCLUSAO", TimeUtils.getNow()).set("STATUSPROCESSAMENTO", statusProcessamento).save();

			}
			
			System.out.println("[Sattva] - Pos loop");

		} catch (Exception e) {
			System.out.println("[SattvaErrorSplit] - " + e.toString());
			e.printStackTrace();
		} finally {
			System.out.println("=============================== Finalizando job split ===============================");
		}
	}
	
	private void authenticate(BigDecimal codigoUsuario) throws Exception {
		this.oldAuthInfo = AuthenticationInfo.getCurrentOrNull();

		if (this.oldAuthInfo != null) {
			AuthenticationInfo.unregistry();
		}

		DynamicVO usuarioVO = (DynamicVO) EntityFacadeFactory.getDWFFacade()
				.findEntityByPrimaryKeyAsVO(DynamicEntityNames.USUARIO, new Object[] { codigoUsuario });

		StringBuffer authID = new StringBuffer();
		authID.append(System.currentTimeMillis()).append(':').append(usuarioVO.asBigDecimal("CODUSU")).append(':')
				.append(this.hashCode());
		this.authInfo = new AuthenticationInfo(usuarioVO.asString("NOMEUSU"), usuarioVO.asBigDecimalOrZero("CODUSU"),
				usuarioVO.asBigDecimalOrZero("CODGRUPO"), new Integer(authID.toString().hashCode()));
		this.authInfo.makeCurrent();

		final ServiceContext sctx = new ServiceContext(null);
		sctx.setAutentication(this.authInfo);
		sctx.makeCurrent();

		JapeSessionContext.putProperty("usuario_logado", this.authInfo.getUserID());
		JapeSessionContext.putProperty("dh_atual", new Timestamp(System.currentTimeMillis()));
		JapeSessionContext.putProperty("d_atual", new Timestamp(TimeUtils.getToday()));
		JapeSessionContext.putProperty("usuarioVO", usuarioVO);
		JapeSessionContext.putProperty("authInfo", this.authInfo);
	}

	private AuthenticationInfo oldAuthInfo;
	private AuthenticationInfo authInfo;

	private BigDecimal verificaSaldoEstoqueAgrupando(BigDecimal codEmp, BigDecimal codProd) throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		NativeSql sqlEstoqueDisponivel = new NativeSql(dwf.getJdbcWrapper());
		sqlEstoqueDisponivel.loadSql(getClass(), "qryEstoqueDisponivel.sql");
		sqlEstoqueDisponivel.setNamedParameter("CODEMP", codEmp);
		sqlEstoqueDisponivel.setNamedParameter("CODPROD", codProd);

		ResultSet rs = sqlEstoqueDisponivel.executeQuery();
		if (rs.next()) {
			return rs.getBigDecimal(1);
		} else {
			return BigDecimal.ZERO;
		}
	}

	private String buscaCidadeDestino(BigDecimal codCidadeDestino) throws Exception {
		if (codCidadeDestino == null) {
			return "";
		}
//		JapeWrapper cidadeDAO = JapeFactory.dao("Cidade");
		
		PersistentLocalEntity registroPLE = dwf.findEntityByPrimaryKey("Cidade", codCidadeDestino);
		DynamicVO cidadeVO = (DynamicVO) registroPLE.getValueObject();
		
//		DynamicVO cidadeVO = cidadeDAO.findOne("CODCID = ?", codCidadeDestino);
		return cidadeVO.asString("NOMECID");
	}

	private boolean verificaPedidoFaturado(BigDecimal nuNotaPreSplit) throws Exception {
		JapeWrapper tgfvarDAO = JapeFactory.dao("CompraVendavariosPedido");
		
		FinderWrapper finderPedidoFaturado = new FinderWrapper("CompraVendavariosPedido", "this.NUNOTAORIG = ?", new Object[] {nuNotaPreSplit});
		Collection<PersistentLocalEntity> colletionPLE = dwf.findByDynamicFinder(finderPedidoFaturado);
		
		Collection<DynamicVO> tgfvarColl = new ArrayList<DynamicVO>();
		for (PersistentLocalEntity itemPLE : colletionPLE) {
			tgfvarColl.add((DynamicVO) itemPLE.getValueObject());
		}
		
		DynamicVO tgfvarVO = tgfvarDAO.findOne("NUNOTAORIG = ?", nuNotaPreSplit);

		if (tgfvarVO == null) {
			return false;
		}

		return true;
	}

	private void imprimeSplitFinal(Collection<Split> pedidosSplit, Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa)
			throws Exception {
		for (Split pedido : pedidosSplit) {
			for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {

				if (nuNotaEmp.getKey().intValue() == 1 && pedido.codEmp.intValue() == 1) {
					log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " + pedido.toString();
				}
				if (nuNotaEmp.getKey().intValue() == 5 && pedido.codEmp.intValue() == 5) {
					log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " + pedido.toString();
				}
			}
		}
	}

	private void doSplit(String regraPrioridade, Collection<DynamicVO> itensPreSplit, BigDecimal nuNotaPreSplit,
			Collection<Transferencia> itensTransferencia, Collection<Split> splitPedidos) throws Exception {
//		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		for (DynamicVO itemPreSplitVO : itensPreSplit) {
			BigDecimal qtdNeg = itemPreSplitVO.asBigDecimal("QTDNEG");
			BigDecimal codProd = itemPreSplitVO.asBigDecimal("CODPROD");
			String nomeProduto = itemPreSplitVO.asString("Produto.DESCRPROD");
			BigDecimal saldo = qtdNeg;
			
			PersistentLocalEntity registroPLE = dwf.findEntityByPrimaryKey("Produto", codProd);
			DynamicVO produtoVO = (DynamicVO) registroPLE.getValueObject();

//			DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
			if ("D".equals(produtoVO.asString("USOPROD"))) {
				continue;
			}

			BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
			BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd);
			BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);

			if ((estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() < qtdNeg
					.doubleValue()) {
				log += "\n Quantidade do Produto: " + codProd + ", [QtdNeg: " + qtdNeg
						+ "] é maior que a quantidade geral do estoque ["
						+ (estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() + "]";
				continue;
			}

			if (regraPrioridade.equals("516")) {
				log += "\n\nProduto: " + codProd + ", QtdNeg: " + qtdNeg + "\nDisponibilidade(5): " + estDisponivelEmp5
						+ ", Disponibilidade(1): " + estDisponivelEmp1 + ", Disponibilidade(6): " + estDisponivelEmp6;
			} else {
				log += "\n\nProduto: " + codProd + ", QtdNeg: " + qtdNeg + "\nDisponibilidade(1): " + estDisponivelEmp1
						+ ", Disponibilidade(6): " + estDisponivelEmp6 + ", Disponibilidade(5): " + estDisponivelEmp5;
			}

			if (regraPrioridade.equals("165")) {
				BigDecimal saldoPendente = verificaDisponibilidade(empresa1, saldo, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos);
				saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos);
				saldoPendente = verificaDisponibilidade(empresa5, saldoPendente, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos);
			}

			if (regraPrioridade.equals("516")) {
				BigDecimal saldoPendente = verificaDisponibilidade(empresa5, saldo, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos);
				saldoPendente = verificaDisponibilidade(empresa1, saldoPendente, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos);
				saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos);
			}

		}

	}

	private BigDecimal verificaDisponibilidade(BigDecimal empresa, BigDecimal saldo, BigDecimal estDisponivel,
			boolean realizaTransferencia, Collection<Transferencia> itensTransferencia, BigDecimal codProd,
			Collection<Split> splitPedidos) {
		// Estoque Total disponivel
		if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0
				&& estDisponivel.doubleValue() >= saldo.doubleValue()) {
			if (realizaTransferencia) {
				geraTransferencia = true;
				itensTransferencia.add(new Transferencia(codProd, saldo));
				splitPedidos.add(new Split(empresa1, codProd, saldo));
				log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
				log += "\nFazendo transferencia de " + saldo + " da empresa 6 para 1";
			} else {
				splitPedidos.add(new Split(empresa, codProd, saldo));
				log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
			}
			return BigDecimal.ZERO;
		}

		// Estoque Parcial disponivel
		if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0
				&& estDisponivel.doubleValue() < saldo.doubleValue()) {
			if (realizaTransferencia) {
				geraTransferencia = true;
				itensTransferencia.add(new Transferencia(codProd, estDisponivel));
				splitPedidos.add(new Split(empresa1, codProd, estDisponivel));
				log += "\nEstoque na empresa " + empresa + " com disponibilidade parcial. Saldo pendente: "
						+ saldo.subtract(estDisponivel);
				log += "\nFazendo transferencia de " + estDisponivel + " da empresa 6 para 1";
			} else {
				splitPedidos.add(new Split(empresa, codProd, estDisponivel));
				log += "\nEstoque na empresa: " + empresa + " com disponibilidade parcial. Saldo pendente: "
						+ saldo.subtract(estDisponivel);
			}
		}

		return saldo.subtract(estDisponivel);
	}

}
