package br.com.sattva.tdb.acoes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sattva.tdb.job.Split;
import br.com.sattva.tdb.job.Transferencia;
import br.com.sattva.tdb.utils.TdbHelper;

public class SplitPedidosAcao implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao arg0) throws Exception {
		final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
		final BigDecimal empresa1 = new BigDecimal("1"); // TDB
		final BigDecimal empresa6 = new BigDecimal("6"); // Loja
			
		String nuNotaString = arg0.getParam("NUNOTA")+"";
		final BigDecimal nuNota = new BigDecimal(nuNotaString);
		
		SessionHandle hnd = null;
		
		try {
			
			hnd = JapeSession.open();
            hnd.execWithTX(new JapeSession.TXBlock() {
            	
            	JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
    	        JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
    	        JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");

	            public void doWithTx() throws Exception {
	
	            	String log = "";
	            	String filtroCabecalho = "PENDENTE = 'S' "
                    				+ "AND CODVEND = 14 AND CODCIDDESTINO NOT IN (4798, 2475) "
                    				+ "AND CODTIPOPER = 3102 AND PENDENTE='S' "
                    				+ "AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) "
                    				+ "AND BH_LOJA='TUDO DE BICHO' AND DTNEG >= '02/02/2022' AND CODEMP = 1";
	            	
	            	filtroCabecalho = "NUNOTA = " + nuNota;
	            	
                    Collection<DynamicVO> pedidosValidos = cabecalhoDAO.find(filtroCabecalho);
                    for (DynamicVO pedidoVO : pedidosValidos) {//Ordem de checagem de estoque: Extrema(5), TDB(1), Loja(6)
                    	
                    	Collection<DynamicVO> itensPedido = itemDAO.find("NUNOTA = ?", pedidoVO.asBigDecimal("NUNOTA"));
                    	Collection<Split> splitPedidos = new ArrayList<Split>();
                    	Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
                    	
                    	for (DynamicVO itemPedidoVO : itensPedido) {
                    		BigDecimal codProd = itemPedidoVO.asBigDecimal("CODPROD");
                    		BigDecimal qtdNeg = itemPedidoVO.asBigDecimal("QTDNEG");
                    		BigDecimal saldo = qtdNeg;
                    		
                    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
                    		BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd);
                    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);
                    		
                    		log = "QtdNeg: " + qtdNeg 
                    				+ ", estoqueDisponivelEmp5: " + estDisponivelEmp5 
                    				+ ", estoqueDisponivelEmp1: " + estDisponivelEmp1
                    				+ ", estoqueDisponivelEmp6: " + estDisponivelEmp6;
                    		
                    		boolean temDisponivelEmp5 = 
                    				estDisponivelEmp5.doubleValue() > 0 
                    				&& saldo.doubleValue() <= estDisponivelEmp5.doubleValue();
                    		
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
                    					&& saldo.doubleValue() <= estDisponivelEmp1.doubleValue();
                    										
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
                    	
                    	BigDecimal nuNotaTransferencia = TdbHelper.transfereSaldo6x1(itensTransferencia);
                    	log += "\nNro. Unico. Transferencia: " + nuNotaTransferencia;
                    	
                    	Collection<Split> pedidosSplit = TdbHelper.agrupaSplitPorEmpresa(splitPedidos);

                    	TdbHelper.geraLancamentosSplit(pedidoVO, pedidosSplit);
                    	
                    	for (Split pedido : pedidosSplit) {
                    		log += "\n" + pedido.toString();
                    	}
                    }
                    
                    logDAO.create().set("DESCRICAO", log.toCharArray()).save();
	            }
	    });
			
		} catch (Exception e) {
			arg0.mostraErro(e.toString());
			System.out.println("[Sattva]Split Error: " + e.toString());
		}finally {
			JapeSession.close(hnd);
		}
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
}
