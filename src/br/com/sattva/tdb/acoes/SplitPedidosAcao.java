package br.com.sattva.tdb.acoes;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sattva.tdb.job.Split;
import br.com.sattva.tdb.utils.TdbHelper;

public class SplitPedidosAcao implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao arg0) throws Exception {
		
//		try {
			
		
		
		String nuNotaString = arg0.getParam("NUNOTA")+"";
		final BigDecimal nuNota = new BigDecimal(nuNotaString);
		
		SessionHandle hnd = null;
		
		try {
			
			hnd = JapeSession.open();
			
            hnd.execWithTX(new JapeSession.TXBlock() {
            	
            	JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
    	        JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
    	        JapeWrapper logDAO = JapeFactory.dao("AD_LOGTEMP");

	            public void doWithTx() throws Exception {
	
	            	String filtroCabecalho = "PENDENTE = 'S' "
                    				+ "AND CODVEND = 14 AND CODCIDDESTINO NOT IN (4798, 2475) "
                    				+ "AND CODTIPOPER = 3102 AND PENDENTE='S' "
                    				+ "AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) "
                    				+ "AND BH_LOJA='TUDO DE BICHO' AND DTNEG >= '02/02/2022' AND CODEMP = 1";
	            	
	            	filtroCabecalho = "NUNOTA = " + nuNota;
	            	
                    Collection<DynamicVO> pedidosValidos = cabecalhoDAO.find(filtroCabecalho);
                    
                    for (DynamicVO pedidoVO : pedidosValidos) {//Ordem de checagem de estoque: Extrema(5), TDB(1), Loja(6)
                    	
                    	Collection<DynamicVO> itensPedido = itemDAO.find("NUNOTA = ?", pedidoVO.asBigDecimal("NUNOTA"));
                    	Collection<Split> quebraPedido = new ArrayList();
                    	
                    	for (DynamicVO itemPedidoVO : itensPedido) {
                    		BigDecimal codEmp = itemPedidoVO.asBigDecimal("CODEMP");
                    		BigDecimal codProd = itemPedidoVO.asBigDecimal("CODPROD");
                    		BigDecimal qtdNeg = itemPedidoVO.asBigDecimal("QTDNEG");
                    		BigDecimal saldo = qtdNeg;
                    		
                    		logDAO.create().set("DESCRICAO", ("QtdNeg: " + qtdNeg).toCharArray()).save();
                    		
                    		BigDecimal estoqueDisponivelEmp5 = TdbHelper.verificaSaldoEstoque(codEmp, codProd);
                    		BigDecimal estoqueDisponivelEmp1 = TdbHelper.verificaSaldoEstoque(codEmp, codProd);
                    		BigDecimal estoqueDisponivelEmp6 = TdbHelper.verificaSaldoEstoque(codEmp, codProd);
                    		
                    		logDAO.create().set("DESCRICAO", ("estoqueDisponivelEmp5: " + estoqueDisponivelEmp5).toCharArray()).save();
                    		logDAO.create().set("DESCRICAO", ("estoqueDisponivelEmp1: " + estoqueDisponivelEmp1).toCharArray()).save();
                    		logDAO.create().set("DESCRICAO", ("estoqueDisponivelEmp6: " + estoqueDisponivelEmp6).toCharArray()).save();
                    		
                    		if (saldo.doubleValue() <= estoqueDisponivelEmp5.doubleValue()) {
                    			Split quebraEmp5 = new Split();
                    			quebraEmp5.codEmp = new BigDecimal("5");
                    			quebraEmp5.codProd = codProd;
                    			quebraEmp5.qtdNeg = saldo; 
                    			quebraPedido.add(quebraEmp5);
                    		} else {
                    			Split quebraEmp5 = new Split();
                    			quebraEmp5.codEmp = new BigDecimal("5");
                    			quebraEmp5.codProd = codProd;
                    			quebraEmp5.qtdNeg = estoqueDisponivelEmp5; 
                    			quebraPedido.add(quebraEmp5);
                    			
                    			saldo = qtdNeg.subtract(estoqueDisponivelEmp5);
                    			
                    			if (saldo.doubleValue() <= estoqueDisponivelEmp1.doubleValue() && saldo.doubleValue() > 0) {
                    				Split quebraEmp1 = new Split();
                        			quebraEmp1.codEmp = new BigDecimal("1");
                        			quebraEmp1.codProd = codProd;
                        			quebraEmp1.qtdNeg = saldo; 
                        			quebraPedido.add(quebraEmp1);
                    			} else {
                    				Split quebraEmp1 = new Split();
                        			quebraEmp1.codEmp = new BigDecimal("1");
                        			quebraEmp1.codProd = codProd;
                        			quebraEmp1.qtdNeg = estoqueDisponivelEmp1; 
                        			quebraPedido.add(quebraEmp1);
                        			
                        			saldo = saldo.subtract(estoqueDisponivelEmp1);
                        			
                        			if (saldo.doubleValue() <= estoqueDisponivelEmp6.doubleValue()) {
                        				TdbHelper.transfereSaldo6x1(codProd, saldo);
                        				
                        				Split quebraEmp6 = new Split();
                            			quebraEmp6.codEmp = new BigDecimal("1");
                            			quebraEmp6.codProd = codProd;
                            			quebraEmp6.qtdNeg = saldo; 
                            			quebraPedido.add(quebraEmp6);
                        				
                        			} else {
                        				TdbHelper.registraLogSplit("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo");
                        			}
                    			}
                    		}
                    	}
                    	
                    	for (Split pedido : quebraPedido) {
                    		TdbHelper.geraLancamentosSplit(pedidoVO.asBigDecimal("NUNOTA"), pedido);
//                    		System.out.println(pedido.toString());
                    		
                    		logDAO.create().set("DESCRICAO", pedido.toString().toCharArray()).save();
                    		
                    	}
                    }
	            }
	
	    });
			
		} catch (Exception e) {
			/*
			JapeWrapper log2DAO = JapeFactory.dao("AD_LOGTEMP");
			try {
				log2DAO.create().set("DESCRICAO", "Split Error: " + e.toString().toCharArray()).save();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
			arg0.mostraErro(e.toString());
			System.out.println("Split Error: " + e.toString());
		}finally {
			JapeSession.close(hnd);
		}
		
//	} catch (Exception e) {
//		arg0.mostraErro(e.toString());
//	}
	} 
}