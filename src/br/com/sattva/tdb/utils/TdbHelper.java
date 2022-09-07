package br.com.sattva.tdb.utils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sankhya.modelcore.comercial.PrecoCustoHelper;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;
import br.com.sankhya.modelcore.helper.CalculoPrecosCustosHelper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.MGEComercialUtil;
import br.com.sankhya.modelcore.util.MGECoreParameter;
import br.com.sattva.tdb.job.Split;
import br.com.sattva.tdb.job.Transferencia;

public class TdbHelper {
	final static BigDecimal codLocal = new BigDecimal("1001"); 
	
	public static BigDecimal verificaSaldoEstoque(BigDecimal codEmp, BigDecimal codProd) throws Exception {
		ComercialUtils.ResultadoValidaEstoque resulEstoque;
		resulEstoque = ComercialUtils.validaEstoque(codEmp, codLocal, codProd, " ", null);
		return resulEstoque.getQtdEst();

	}
	
	public BigDecimal verificaSaldoEstoqueAgrupando(BigDecimal codEmp, BigDecimal codProd, BigDecimal codLocal) throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		NativeSql sqlEstoqueDisponivel = new NativeSql(dwf.getJdbcWrapper());
		sqlEstoqueDisponivel.loadSql(getClass(), "QueryBensFaturaveis.sql");
		sqlEstoqueDisponivel.setNamedParameter("CODEMP", codEmp);
		sqlEstoqueDisponivel.setNamedParameter("CODPROD", codProd);
		sqlEstoqueDisponivel.setNamedParameter("CODLOCAL", codLocal);
		
		ResultSet rs = sqlEstoqueDisponivel.executeQuery();
		if (rs.next()) {
			return rs.getBigDecimal(1);
		} else {
			return BigDecimal.ZERO;
		}
	}

	public static void transfereSaldo6x1(BigDecimal codProd, BigDecimal saldo) throws Exception {
		// TODO Auto-generated method stub
		
		validaParametros();
		BigDecimal nuNotaModeloSaida = buscaNunotaModeloSaida();
		JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
		DynamicVO cabModeloVO = cabecalhoDAO.findOne("NUNOTA = ?", nuNotaModeloSaida);
		
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		DynamicVO notaTransferenciaVO = (DynamicVO) dwf.getDefaultValueObjectInstance("CabecalhoNota");
		notaTransferenciaVO.setProperty("CODEMP", cabModeloVO.asBigDecimalOrZero("CODEMP"));
		notaTransferenciaVO.setProperty("CODPARC", cabModeloVO.asBigDecimalOrZero("CODPARC"));
		notaTransferenciaVO.setProperty("OBSERVACAO", "Pedido gerado automaticamente para suprimento de estoque na empresa 1");
		notaTransferenciaVO.setProperty("CODTIPOPER", cabModeloVO.asBigDecimalOrZero("CODTIPOPER"));
		notaTransferenciaVO.setProperty("DHTIPOPER", cabModeloVO.asTimestamp("DHTIPOPER"));				
		notaTransferenciaVO.setProperty("CODTIPVENDA", cabModeloVO.asBigDecimalOrZero("CODTIPVENDA"));
		notaTransferenciaVO.setProperty("DHTIPVENDA", cabModeloVO.asTimestamp("DHTIPVENDA"));
		notaTransferenciaVO.setProperty("CODNAT", cabModeloVO.asBigDecimalOrZero("CODNAT"));
		notaTransferenciaVO.setProperty("CODCENCUS", cabModeloVO.asBigDecimalOrZero("CODCENCUS"));
		notaTransferenciaVO.setProperty("DTNEG", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("DTENTSAI", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("DTFATUR", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("NUMNOTA", BigDecimal.ZERO);
		dwf.createEntity("CabecalhoNota", (EntityVO) notaTransferenciaVO);
	}

	private static void validaParametros() throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		NativeSql qryVerificaParametro = new NativeSql(dwf.getJdbcWrapper());
		
		String selectParametro = "SELECT 1 FROM TSIPAR WHERE CHAVE = 'MODNOTATRANSFS'";
		String insertParametro = "Insert into TSIPAR (CHAVE, DESCRICAO, CODUSU, TIPO, MODULO, CLASSE, ABA, LOGICO, INTEIRO) Values ('MODNOTATRANSFS', 'Modelo de nota para transferencia (Saida)', 0, 'I', 'E', 'Especiais', 'Personalizados', 'S', NULL)";
		
		ResultSet rs = qryVerificaParametro.executeQuery(selectParametro);
		
		if (!rs.next()) {
			qryVerificaParametro.executeUpdate(insertParametro);		
		}
	}
	
	private static BigDecimal buscaNunotaModeloSaida() throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		NativeSql qryVerificaParametro = new NativeSql(dwf.getJdbcWrapper());
		
		String selectParametro = "SELECT INTEIRO FROM TSIPAR WHERE CHAVE = 'MODNOTATRANSFS'";		
		ResultSet rs = qryVerificaParametro.executeQuery(selectParametro);
		
		if (rs.next()) {
			return rs.getBigDecimal("INTEIRO");		
		}
		return BigDecimal.ZERO;
	}

	public static void registraLogSplit(String string) {
		// TODO Auto-generated method stub
	}

	public static Map<BigDecimal, BigDecimal> geraLancamentosSplit(DynamicVO pedidoVO, Collection<Split> pedidosSplit) throws Exception {

		ArrayList<BigDecimal> nroUnicoNovosPedidos = new ArrayList<BigDecimal>();
		ArrayList<BigDecimal> empresasCabecalho = new ArrayList<BigDecimal>();
		Map<BigDecimal, BigDecimal> nuNotaCodEmp = new HashMap<BigDecimal, BigDecimal>();

		empresasCabecalho.addAll(separaEmpresasCabecalho(pedidosSplit));
		
		for (BigDecimal codEmp : empresasCabecalho) {
			Map<String, Object> trocaInformacoesCab = new HashMap<>();
			trocaInformacoesCab.put("CODEMP", codEmp);
			trocaInformacoesCab.put("CODEMPNEGOC", codEmp);
//			trocaInformacoesCab.put("OBSERVACAO", "Pedido gerado automaticamento pela regra de Split");	
			trocaInformacoesCab.put("ORDEMCARGA", null);
			trocaInformacoesCab.put("CODTIPOPER", new BigDecimal("3132"));
			trocaInformacoesCab.put("DHTIPOPER", getDhTipOper(new BigDecimal("3132")));
			
			if (codEmp.intValue() != 5) {
				trocaInformacoesCab.put("VLRFRETE", BigDecimal.ZERO);
				
			} else {
				
				String transportadoras = "'CORREIOS'-'CORREIOS PAC'-'CORREIOS SEDEX'";
				System.out.println("[Stva1]");
				String bhMetodo = "";
				
				if(pedidoVO.asString("BH_METODO") == null) {
					
				} else {
					bhMetodo = transportadoras.indexOf(pedidoVO.asString("BH_METODO")) > -1 ? pedidoVO.asString("BH_METODO") : "JADLOG PACKAGE";					
				}
				
				System.out.println("[Stva2]");
				BigDecimal codParcTrans = BigDecimal.ZERO;
				if(bhMetodo!= null) {
					codParcTrans = localizaTransportadoraByMetodo(bhMetodo);										
				}
				
				trocaInformacoesCab.put("VLRFRETE", pedidoVO.asBigDecimal("VLRFRETE"));
				trocaInformacoesCab.put("BH_METODO", bhMetodo);
				trocaInformacoesCab.put("CODPARCTRANSP", codParcTrans);
			}
			
			Map<String, Object> pkNewNuNota = CentralNotasUtils.duplicaRegistro(pedidoVO, "CabecalhoNota", trocaInformacoesCab);
			nroUnicoNovosPedidos.add((BigDecimal) pkNewNuNota.get("NUNOTA"));
			nuNotaCodEmp.put(codEmp, (BigDecimal) pkNewNuNota.get("NUNOTA"));

			
			for (Split pedido : pedidosSplit) {
				if (pedido.codEmp.intValue() == codEmp.intValue()) {
					
					insereItensEmpresa(pkNewNuNota, codEmp, pedido.codProd, pedido.qtdNeg, pedidoVO.asBigDecimal("NUNOTA"));						
					
				}
			}
		}
		
		vinculaTgfvar(nroUnicoNovosPedidos, pedidoVO.asBigDecimal("NUNOTA"));
		
		for (BigDecimal nroUnico : nroUnicoNovosPedidos) {
			recalculaImpostoFinanceiro(nroUnico);
		}
		
		return nuNotaCodEmp;
		
	}

	private static BigDecimal localizaTransportadoraByMetodo(String bhMetodo) throws Exception {
		JapeWrapper parceiroDAO = JapeFactory.dao("Parceiro");		
		DynamicVO parceiroVO = parceiroDAO.findOne("AD_METODOSDEENVIO = ?", bhMetodo);
		if (parceiroVO == null) {
			return BigDecimal.ZERO;
		}
		return parceiroVO.asBigDecimal("CODPARC");
	}

	private static void recalculaImpostoFinanceiro(BigDecimal nroUnico) throws Exception {
		ImpostosHelpper ih = new ImpostosHelpper();
		ih.setForcarRecalculo(true);
		ih.calcularImpostos(nroUnico);

		CentralFinanceiro financeiro = new CentralFinanceiro();		
		financeiro.inicializaNota(nroUnico);
		financeiro.refazerFinanceiro();
		
	}

	private static void vinculaTgfvar(ArrayList<BigDecimal> nroUnicoNovosPedidos, BigDecimal nuNotaOriginal) throws Exception {
		JapeWrapper tgfvarDAO = JapeFactory.dao("CompraVendavariosPedido");
		JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
		
		System.out.println("[SattvaLog9] ");
		
		for (BigDecimal nroUnico : nroUnicoNovosPedidos) {
			
			Collection<DynamicVO> itensNewVO = itemDAO.find("NUNOTA = ?", nroUnico);
			for (DynamicVO itemNewVO : itensNewVO) {
				
				DynamicVO itemOldVO = itemDAO.findOne("NUNOTA = ? AND CODPROD = ?", nuNotaOriginal, itemNewVO.asBigDecimal("CODPROD"));
				
				tgfvarDAO.create()
				.set("NUNOTA", nroUnico)
				.set("SEQUENCIA", itemNewVO.asBigDecimal("SEQUENCIA"))
				.set("NUNOTAORIG", itemOldVO.asBigDecimal("NUNOTA"))
				.set("SEQUENCIAORIG", itemOldVO.asBigDecimal("SEQUENCIA"))
				.set("QTDATENDIDA", itemNewVO.asBigDecimal("QTDNEG"))
				.save();
				
			}
			
		}
		
	}

	private static void insereItensEmpresa(Map<String, Object> pkNewNuNota, BigDecimal codEmp, BigDecimal codProd, BigDecimal qtdNeg, BigDecimal nuNotaOrig) throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		
		JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
		DynamicVO itemOrigemVO = itemDAO.findOne("NUNOTA = ? AND CODPROD = ?", nuNotaOrig, codProd);
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
		DynamicVO itemVO = (DynamicVO) dwf.getDefaultValueObjectInstance("ItemNota");
		itemVO.setProperty("NUNOTA", pkNewNuNota.get("NUNOTA"));
		itemVO.setProperty("CODEMP", codEmp);
		itemVO.setProperty("CODPROD", codProd);
		itemVO.setProperty("QTDNEG", qtdNeg);
		itemVO.setProperty("CODLOCALORIG", itemOrigemVO.asBigDecimal("CODLOCALORIG"));
		itemVO.setProperty("VLRUNIT", itemOrigemVO.asBigDecimal("VLRUNIT"));
		itemVO.setProperty("VLRTOT", itemOrigemVO.asBigDecimal("VLRUNIT").multiply(qtdNeg));
		itemVO.setProperty("CODVOL", produtoVO.asString("CODVOL"));
		itemVO.setProperty("ATUALESTOQUE", BigDecimal.ONE);
		itemVO.setProperty("RESERVA", "S");
		dwf.createEntity("ItemNota", (EntityVO) itemVO);
		
	}

	private static Collection<? extends BigDecimal> separaEmpresasCabecalho(Collection<Split> pedidosSplit) {
		ArrayList<BigDecimal> empresasList = new ArrayList<BigDecimal>();
		for (Split empresas : pedidosSplit) {
			empresasList.add(empresas.codEmp);
		}
		ArrayList<BigDecimal> distinctEmp = TdbHelper.removeDuplicates(empresasList);
		
		return distinctEmp;
		
		
	}

	public static Map<String, BigDecimal> transfereSaldo6x1(Collection<Transferencia> itensTransferencia) throws Exception {
		
		Map<String, BigDecimal> notasTransferenca = new HashMap<String, BigDecimal>();
		
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
		final BigDecimal empresaOrigem = new BigDecimal("6");
		
		validaParametros();

		BigDecimal nuNotaModeloSaida = buscaNunotaModeloSaida();
		DynamicVO cabModeloVO = cabecalhoDAO.findOne("NUNOTA = ?", nuNotaModeloSaida);
		
		DynamicVO notaTransferenciaVO = (DynamicVO) dwf.getDefaultValueObjectInstance("CabecalhoNota");
		notaTransferenciaVO.setProperty("CODEMP", cabModeloVO.asBigDecimalOrZero("CODEMP"));
		notaTransferenciaVO.setProperty("CODPARC", cabModeloVO.asBigDecimalOrZero("CODPARC"));
		notaTransferenciaVO.setProperty("OBSERVACAO", "Pedido gerado automaticamente para suprimento de estoque na empresa 1");
		notaTransferenciaVO.setProperty("CODTIPOPER", cabModeloVO.asBigDecimalOrZero("CODTIPOPER"));
		notaTransferenciaVO.setProperty("DHTIPOPER", cabModeloVO.asTimestamp("DHTIPOPER"));				
		notaTransferenciaVO.setProperty("CODTIPVENDA", cabModeloVO.asBigDecimalOrZero("CODTIPVENDA"));
		notaTransferenciaVO.setProperty("DHTIPVENDA", cabModeloVO.asTimestamp("DHTIPVENDA"));
		notaTransferenciaVO.setProperty("CODNAT", cabModeloVO.asBigDecimalOrZero("CODNAT"));
		notaTransferenciaVO.setProperty("CODCENCUS", cabModeloVO.asBigDecimalOrZero("CODCENCUS"));
		notaTransferenciaVO.setProperty("DTNEG", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("DTENTSAI", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("DTFATUR", TimeUtils.getNow());
		notaTransferenciaVO.setProperty("NUMNOTA", BigDecimal.ZERO);
		dwf.createEntity("CabecalhoNota", (EntityVO) notaTransferenciaVO);
		
		DynamicVO pedidoCompraTransfVO = (DynamicVO) dwf.getDefaultValueObjectInstance("CabecalhoNota");
		pedidoCompraTransfVO.setProperty("CODEMP", BigDecimal.ONE);
		pedidoCompraTransfVO.setProperty("CODPARC", new BigDecimal("263436"));
		pedidoCompraTransfVO.setProperty("OBSERVACAO", "Pedido gerado automaticamente para suprimento de estoque na empresa 1");
		pedidoCompraTransfVO.setProperty("CODTIPOPER", new BigDecimal("2000"));
		pedidoCompraTransfVO.setProperty("DHTIPOPER", getDhTipOper(new BigDecimal("2000")));				
		pedidoCompraTransfVO.setProperty("CODTIPVENDA", cabModeloVO.asBigDecimalOrZero("CODTIPVENDA"));
		pedidoCompraTransfVO.setProperty("DHTIPVENDA", cabModeloVO.asTimestamp("DHTIPVENDA"));
		pedidoCompraTransfVO.setProperty("CODNAT", cabModeloVO.asBigDecimalOrZero("CODNAT"));
		pedidoCompraTransfVO.setProperty("CODCENCUS", cabModeloVO.asBigDecimalOrZero("CODCENCUS"));
		pedidoCompraTransfVO.setProperty("DTNEG", TimeUtils.getNow());
		pedidoCompraTransfVO.setProperty("DTENTSAI", TimeUtils.getNow());
		pedidoCompraTransfVO.setProperty("DTFATUR", TimeUtils.getNow());
		pedidoCompraTransfVO.setProperty("NUMNOTA", BigDecimal.ZERO);
		dwf.createEntity("CabecalhoNota", (EntityVO) pedidoCompraTransfVO);
		
		BigDecimal nuNotaTransfEntrada = pedidoCompraTransfVO.asBigDecimal("NUNOTA");
		BigDecimal nuNotaTransfSaida = notaTransferenciaVO.asBigDecimal("NUNOTA");
		notasTransferenca.put("NUNOTATRANSFENTRADA", nuNotaTransfEntrada);
		notasTransferenca.put("NUNOTATRANSFSAIDA", nuNotaTransfSaida);
		
		for (Transferencia item : itensTransferencia) {
						
			BigDecimal custoSemIcms = ComercialUtils.getUltimoCusto(item.codProd, empresaOrigem, codLocal, " ", "CUSSEMICM");
			
			DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", item.codProd);
			DynamicVO itemSaidaVO = (DynamicVO) dwf.getDefaultValueObjectInstance("ItemNota");
			itemSaidaVO.setProperty("NUNOTA", nuNotaTransfSaida);
			itemSaidaVO.setProperty("CODEMP", empresaOrigem);
			itemSaidaVO.setProperty("CODPROD", item.codProd);
			itemSaidaVO.setProperty("QTDNEG", item.qtdNeg);
			itemSaidaVO.setProperty("VLRUNIT", custoSemIcms);
			itemSaidaVO.setProperty("VLRTOT", custoSemIcms.multiply(item.qtdNeg));
			itemSaidaVO.setProperty("CODVOL", produtoVO.asString("CODVOL"));
			itemSaidaVO.setProperty("ATUALESTOQUE", BigDecimal.ONE);
			itemSaidaVO.setProperty("RESERVA", "S");
			dwf.createEntity("ItemNota", (EntityVO) itemSaidaVO);
			
			DynamicVO itemEntradaVO = (DynamicVO) dwf.getDefaultValueObjectInstance("ItemNota");
			itemEntradaVO.setProperty("NUNOTA", nuNotaTransfEntrada);
			itemEntradaVO.setProperty("CODEMP", BigDecimal.ONE);
			itemEntradaVO.setProperty("CODPROD", item.codProd);
			itemEntradaVO.setProperty("QTDNEG", item.qtdNeg);
			itemEntradaVO.setProperty("VLRUNIT", custoSemIcms);
			itemEntradaVO.setProperty("VLRTOT", custoSemIcms.multiply(item.qtdNeg));
			itemEntradaVO.setProperty("CODVOL", produtoVO.asString("CODVOL"));
			itemEntradaVO.setProperty("ATUALESTOQUE", BigDecimal.ZERO);
			itemEntradaVO.setProperty("RESERVA", "S");
			dwf.createEntity("ItemNota", (EntityVO) itemEntradaVO);
			
			itemSaidaVO.clean();
			itemEntradaVO.clean();
			
		}
		
		return notasTransferenca;
	}
	

	public static Timestamp getDhTipOper(BigDecimal codTipOper) throws Exception {
		JapeWrapper topDAO = JapeFactory.dao("TipoOperacao");
		DynamicVO topVO = topDAO.findOne("CODTIPOPER = ? AND DHALTER = (SELECT MAX(DHALTER) FROM TGFTOP T WHERE T.CODTIPOPER = TGFTOP.CODTIPOPER)", codTipOper);
		return topVO.asTimestamp("DHALTER");
	}

	public static Collection<Split> agrupaSplitPorEmpresa(Collection<Split> quebraPedido) {
		//Separa itens empresa 1 e itens empresa 5
		Collection<Split> itensEmpresa1 = new ArrayList<Split>();
		Collection<Split> itensEmpresa5 = new ArrayList<Split>();
		Collection<Split> itensEmpresa1Agrupado = new ArrayList<Split>();
		Collection<Split> itensEmpresa1Agrupado2 = new ArrayList<Split>();
		Collection<Split> itensEmpresa5Agrupado = new ArrayList<Split>();
		Collection<Split> itensEmpresa5Agrupado2 = new ArrayList<Split>();
		Collection<Split> finalSplit = new ArrayList<Split>();
		
		for (Split pedido : quebraPedido) {	
			if(pedido.qtdNeg.doubleValue() > 0d) {
				if (pedido.codEmp.intValue() == 1) {
					itensEmpresa1.add(pedido);
				} else {
					itensEmpresa5.add(pedido);
				}				
			}
		}
		
		ArrayList<BigDecimal> produtosEmpresa1 = new ArrayList<BigDecimal>();
		for(Split emp1 : itensEmpresa1) {
			produtosEmpresa1.add(emp1.codProd);
		}
		
		ArrayList<BigDecimal> produtosEmpresa5 = new ArrayList<BigDecimal>();
		for(Split emp5 : itensEmpresa5) {
			produtosEmpresa5.add(emp5.codProd);
		}
		
		ArrayList<BigDecimal> produtosDistinctEmp1 = TdbHelper.removeDuplicates(produtosEmpresa1);
		ArrayList<BigDecimal> produtosDistinctEmp5 = TdbHelper.removeDuplicates(produtosEmpresa5);
		
		for (BigDecimal produtoEmpresa1 : produtosDistinctEmp1) {
			Split s = new Split(new BigDecimal("1"), produtoEmpresa1, BigDecimal.ZERO);
			itensEmpresa1Agrupado.add(s);			
		}
		
		for (BigDecimal produtoEmpresa5 : produtosDistinctEmp5) {
			Split s = new Split(new BigDecimal("5"), produtoEmpresa5, BigDecimal.ZERO);
			itensEmpresa5Agrupado.add(s);			
		}
		
		for (Split sp : itensEmpresa1Agrupado) {
			BigDecimal qtdSomada = BigDecimal.ZERO;
			for (Split ie1 : itensEmpresa1) {
				if(ie1.codProd.intValue() == sp.codProd.intValue()) {
					qtdSomada = qtdSomada.add(ie1.qtdNeg);
				}
			}
			
			itensEmpresa1Agrupado2.add(new Split(new BigDecimal("1"), sp.codProd, qtdSomada));
		}
		
		for (Split sp : itensEmpresa5Agrupado) {
			BigDecimal qtdSomada = BigDecimal.ZERO;
			for (Split ie1 : itensEmpresa5) {
				if(ie1.codProd.intValue() == sp.codProd.intValue()) {
					qtdSomada = qtdSomada.add(ie1.qtdNeg);
				}
			}
			
			itensEmpresa5Agrupado2.add(new Split(new BigDecimal("5"), sp.codProd, qtdSomada));
		}
		
		finalSplit.addAll(itensEmpresa1Agrupado2);
		finalSplit.addAll(itensEmpresa5Agrupado2);
		
		return finalSplit;
		
	}
	
	
	public static <T> ArrayList<T> removeDuplicates(ArrayList<T> list) {
        // Create a new ArrayList
        ArrayList<T> newList = new ArrayList<T>();
  
        // Traverse through the first list
        for (T element : list) {
  
            // If this element is not present in newList
            // then add it
            if (!newList.contains(element)) {
                newList.add(element);
            }
        }
  
        // return the new list
        return newList;
    }
	
	public BigDecimal verificaSaldoEstoqueAgrupando(BigDecimal codEmp, BigDecimal codProd) throws Exception {
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

	public String verificaDisponibilidade156(Collection<DynamicVO> itensPedido, String log, BigDecimal nuNotaOrig, Collection<Split> splitPedidos, boolean geraTransferencia, Collection<Transferencia> itensTransferencia) throws Exception {
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
		
		final BigDecimal empresa1 = new BigDecimal("1"); // TDB
		final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
		final BigDecimal empresa6 = new BigDecimal("6"); // Loja
		
		for (DynamicVO itemPedidoVO : itensPedido) {
    		BigDecimal codProd = itemPedidoVO.asBigDecimal("CODPROD");
    		BigDecimal qtdNeg = itemPedidoVO.asBigDecimal("QTDNEG");
    		BigDecimal saldo = qtdNeg;
    		String nomeProduto = itemPedidoVO.asString("Produto.DESCRPROD");
    		
    		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
    		if("D".equals(produtoVO.asString("USOPROD"))) {
    			continue;
    		}
    		
    		BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd);
    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);
    		
    		if (estDisponivelEmp1.doubleValue() == 0 && estDisponivelEmp5.doubleValue() == 0 && estDisponivelEmp6.doubleValue() == 0) {
    			log += "Não possui estoque disponivel em nenhuma empresa para o produto " +codProd +"-"+nomeProduto+ ". Cancelando Split.";
    			logDAO.create()
            	.set("DESCRICAO", log.toCharArray())
            	.set("NUNOTAORIG", nuNotaOrig)
            	.set("DHINCLUSAO", TimeUtils.getNow())
            	.save();
    			return log;
    		}
    		
    		log += "QtdNeg: " + qtdNeg 
    				+ ", estoqueDisponivelEmp5: " + estDisponivelEmp5 
    				+ ", estoqueDisponivelEmp1: " + estDisponivelEmp1
    				+ ", estoqueDisponivelEmp6: " + estDisponivelEmp6;
    		
    		boolean temDisponivelEmp1 = 
    				estDisponivelEmp1.doubleValue() > 0 
    				&& saldo.doubleValue() <= estDisponivelEmp1.doubleValue()
    				&& saldo.doubleValue() > 0;
    		
    		if (temDisponivelEmp1) {
    			
    			log += "\nTEM disponibilidade total empresa 1";
    			
    			Split quebraEmp1 = new Split(new BigDecimal("1"), codProd, saldo);
    			splitPedidos.add(quebraEmp1);
    			
    		} else {
    			
    			Split quebraEmp1 = new Split(new BigDecimal("1"), codProd, estDisponivelEmp1);
    			splitPedidos.add(quebraEmp1);
    			
    			saldo = qtdNeg.subtract(estDisponivelEmp1);
    			
    			log += "\nNÃO TEM disponibilidade total empresa 1" + ", Saldo remanescente: " + saldo;
    			
    			boolean temDisponivelEmp5 = 
    					saldo.doubleValue() > 0 
    					&& estDisponivelEmp5.doubleValue() > 0 
    					&& saldo.doubleValue() <= estDisponivelEmp5.doubleValue()
    					&& saldo.doubleValue() > 0;
    										
    			if (temDisponivelEmp5) {
    				log += "\nTEM disponibilidade total empresa 5";
    				
    				Split quebraEmp5 = new Split(new BigDecimal("5"), codProd, saldo);
        			splitPedidos.add(quebraEmp1);
        			
    			} else {
    				
    				Split quebraEmp5 = new Split(new BigDecimal("5"), codProd, estDisponivelEmp5);
        			splitPedidos.add(quebraEmp5);
        			
        			saldo = saldo.subtract(estDisponivelEmp5);
        			
        			log += "\nNÃO TEM disponibilidade total empresa 5" + ", Saldo remanescente: " + saldo;
        			
        			if (saldo.doubleValue() <= estDisponivelEmp6.doubleValue() && saldo.doubleValue() > 0) {
        				geraTransferencia = true;
        				log += "\nTEM disponibilidade total para transferencia da empresa 6 p/ 1";
        				log += "\nTransferindo quantidade necessária empresa 6 para 1: Quantidade" + saldo;

        				itensTransferencia.add(new Transferencia(codProd, saldo));
        				
        				Split quebraEmp6 = new Split(new BigDecimal("1"), codProd, saldo);
            			splitPedidos.add(quebraEmp6);
            			
        			} else {
        				log += "\nNão é possivel fazer o split pois não tem estoque suficiente em todo o grupo";
        				TdbHelper.registraLogSplit("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo");
        			}
    			}
    		}
    	}
		
		return log;
	}

	public String verificaDisponibilidade516(Collection<DynamicVO> itensPedido, String log, BigDecimal nuNotaOrig, Collection<Split> splitPedidos, boolean geraTransferencia, Collection<Transferencia> itensTransferencia) throws Exception {
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
		
		final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
		final BigDecimal empresa1 = new BigDecimal("1"); // TDB
		final BigDecimal empresa6 = new BigDecimal("6"); // Loja
		
		for (DynamicVO itemPedidoVO : itensPedido) {
    		BigDecimal codProd = itemPedidoVO.asBigDecimal("CODPROD");
    		BigDecimal qtdNeg = itemPedidoVO.asBigDecimal("QTDNEG");
    		BigDecimal saldo = qtdNeg;
    		String nomeProduto = itemPedidoVO.asString("Produto.DESCRPROD");
    		
    		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
    		if("D".equals(produtoVO.asString("USOPROD"))) {
    			continue;
    		}
    		
    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
    		BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd);
    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);
    		
    		if (estDisponivelEmp5.doubleValue() == 0 && estDisponivelEmp1.doubleValue() == 0 && estDisponivelEmp6.doubleValue() == 0) {
    			log += "Não possui estoque disponivel em nenhuma empresa para o produto " +codProd +"-"+nomeProduto+ ". Cancelando Split.";
    			logDAO.create()
            	.set("DESCRICAO", log.toCharArray())
            	.set("NUNOTAORIG", nuNotaOrig)
            	.set("DHINCLUSAO", TimeUtils.getNow())
            	.save();
    			return log;
    		}
    		
    		log += "QtdNeg: " + qtdNeg 
    				+ ", estoqueDisponivelEmp5: " + estDisponivelEmp5 
    				+ ", estoqueDisponivelEmp1: " + estDisponivelEmp1
    				+ ", estoqueDisponivelEmp6: " + estDisponivelEmp6;
    		
    		boolean temDisponivelEmp5 = 
    				estDisponivelEmp5.doubleValue() > 0 
    				&& saldo.doubleValue() <= estDisponivelEmp5.doubleValue()
    				&& saldo.doubleValue() > 0;
    		
    		if (temDisponivelEmp5) {
    			
    			log += "\nTEM disponibilidade total empresa 5";
    			
    			Split quebraEmp5 = new Split(new BigDecimal("5"), codProd, saldo);
    			splitPedidos.add(quebraEmp5);
    			
    		} else {
    			
    			Split quebraEmp5 = new Split(new BigDecimal("5"), codProd, estDisponivelEmp5);
    			splitPedidos.add(quebraEmp5);
    			
    			saldo = qtdNeg.subtract(estDisponivelEmp5);
    			
    			log += "\nNÃO TEM disponibilidade total empresa 5" + ", Saldo remanescente: " + saldo;
    			
    			boolean temDisponivelEmp1 = 
    					saldo.doubleValue() > 0 
    					&& estDisponivelEmp1.doubleValue() > 0 
    					&& saldo.doubleValue() <= estDisponivelEmp1.doubleValue()
    					&& saldo.doubleValue() > 0;
    										
    			if (temDisponivelEmp1) {
    				log += "\nTEM disponibilidade total empresa 1";
    				
    				Split quebraEmp1 = new Split(new BigDecimal("1"), codProd, saldo);
        			splitPedidos.add(quebraEmp1);
        			
    			} else {
    				
    				Split quebraEmp1 = new Split(new BigDecimal("1"), codProd, estDisponivelEmp1);
        			splitPedidos.add(quebraEmp1);
        			
        			saldo = saldo.subtract(estDisponivelEmp1);
        			
        			log += "\nNÃO TEM disponibilidade total empresa 1" + ", Saldo remanescente: " + saldo;
        			
        			if (saldo.doubleValue() <= estDisponivelEmp6.doubleValue() && saldo.doubleValue() > 0) {
        				geraTransferencia = true;
        				log += "\nTEM disponibilidade total para transferencia da empresa 6 p/ 1";
        				log += "\nTransferindo quantidade necessária empresa 6 para 1: Quantidade" + saldo;

        				itensTransferencia.add(new Transferencia(codProd, saldo));
        				
        				Split quebraEmp6 = new Split(new BigDecimal("1"), codProd, saldo);
            			splitPedidos.add(quebraEmp6);
            			
        			} else {
        				log += "\nNão é possivel fazer o split pois não tem estoque suficiente em todo o grupo";
        				TdbHelper.registraLogSplit("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo");
        			}
    			}
    		}
    	}
	return log;
	}
}
