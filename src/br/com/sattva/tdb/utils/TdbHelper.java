package br.com.sattva.tdb.utils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.Collection;

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

	public static void geraLancamentosSplit(BigDecimal asBigDecimal, Split pedido) {
		// TODO Auto-generated method stub
	}

	public static void transfereSaldo6x1(Collection<Transferencia> itensTransferencia) throws Exception {
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
	}
}
