package br.com.sattva.tdb.utils;

import java.math.BigDecimal;
import java.sql.ResultSet;
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
import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sankhya.modelcore.comercial.PrecoCustoHelper;
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

	public static void geraLancamentosSplit(DynamicVO pedidoVO, Collection<Split> pedidosSplit) throws Exception {
		System.out.println("[SattvaLog2.1A] ");
		ArrayList<BigDecimal> nroUnicoNovosPedidos = new ArrayList<BigDecimal>();
		ArrayList<BigDecimal> empresasCabecalho = new ArrayList<BigDecimal>();
		System.out.println("[SattvaLog2.1AB] ");
		empresasCabecalho.addAll(separaEmpresasCabecalho(pedidosSplit));
		
		System.out.println("[SattvaLog2.1] ");
		
		for (BigDecimal codEmp : empresasCabecalho) {
			Map<String, Object> trocaInformacoesCab = new HashMap<>();
			trocaInformacoesCab.put("CODEMP", codEmp);	
			trocaInformacoesCab.put("OBSERVACAO", "Pedido gerado automaticamento pela regra de Split");	
			Map<String, Object> pkNewNuNota = CentralNotasUtils.duplicaRegistro(pedidoVO, "CabecalhoNota", trocaInformacoesCab);
			nroUnicoNovosPedidos.add((BigDecimal) pkNewNuNota.get("NUNOTA"));
			
			System.out.println("[SattvaLog2.2] ");
			
			for (Split pedido : pedidosSplit) {
				if (pedido.codEmp.intValue() == codEmp.intValue()) {
					insereItensEmpresa(pkNewNuNota, codEmp, pedido.codProd, pedido.qtdNeg, pedidoVO.asBigDecimal("NUNOTA"));
				}
			}
			System.out.println("[SattvaLog2.3] ");
		}
		
		for (BigDecimal nroUnico : nroUnicoNovosPedidos) {
			CentralNotasUtils.refazerFinanceiro(nroUnico);
		}
		
	}

	private static void insereItensEmpresa(Map<String, Object> pkNewNuNota, BigDecimal codEmp, BigDecimal codProd, BigDecimal qtdNeg, BigDecimal nuNotaOrig) throws Exception {
		EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
		
		JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
		DynamicVO itemOrigemVO = itemDAO.findOne("NUNOTA = ? AND CODPROD = ?", nuNotaOrig, codProd);
		
		System.out.println("[SattvaLog2.2.1] ");
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
		DynamicVO itemVO = (DynamicVO) dwf.getDefaultValueObjectInstance("ItemNota");
		itemVO.setProperty("NUNOTA", pkNewNuNota.get("NUNOTA"));
		itemVO.setProperty("CODEMP", codEmp);
		itemVO.setProperty("CODPROD", codProd);
		itemVO.setProperty("QTDNEG", qtdNeg);
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

	public static BigDecimal transfereSaldo6x1(Collection<Transferencia> itensTransferencia) throws Exception {
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
		
		BigDecimal nuNotaTransf = notaTransferenciaVO.asBigDecimal("NUNOTA");
		
		for (Transferencia item : itensTransferencia) {
						
			BigDecimal custoSemIcms = ComercialUtils.getUltimoCusto(item.codProd, empresaOrigem, codLocal, " ", "CUSSEMICM");
			
			DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", item.codProd);
			DynamicVO itemVO = (DynamicVO) dwf.getDefaultValueObjectInstance("ItemNota");
			itemVO.setProperty("NUNOTA", nuNotaTransf);
			itemVO.setProperty("CODEMP", empresaOrigem);
			itemVO.setProperty("CODPROD", item.codProd);
			itemVO.setProperty("QTDNEG", item.qtdNeg);
			itemVO.setProperty("VLRUNIT", custoSemIcms);
			itemVO.setProperty("VLRTOT", custoSemIcms.multiply(item.qtdNeg));
			itemVO.setProperty("CODVOL", produtoVO.asString("CODVOL"));
			itemVO.setProperty("ATUALESTOQUE", BigDecimal.ONE);
			itemVO.setProperty("RESERVA", "S");
			dwf.createEntity("ItemNota", (EntityVO) itemVO);
			
			itemVO.clean();
			
		}
		
		return nuNotaTransf;
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
			if (pedido.codEmp.intValue() == 1) {
				itensEmpresa1.add(pedido);
			} else {
				itensEmpresa5.add(pedido);
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
}
