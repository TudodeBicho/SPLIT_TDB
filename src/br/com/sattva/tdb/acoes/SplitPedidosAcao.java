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
		final BigDecimal empresa5 = new BigDecimal("5");
		final BigDecimal empresa1 = new BigDecimal("1");
		final BigDecimal empresa6 = new BigDecimal("6");
		
		
//		try {
			
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
                    	Collection<Split> quebraPedido = new ArrayList();
                    	Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
                    	
                    	for (DynamicVO itemPedidoVO : itensPedido) {
                    		BigDecimal codEmpItem = itemPedidoVO.asBigDecimal("CODEMP");
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
                    		
                    		logDAO.create().set("DESCRICAO", (log).toCharArray()).save();
                    		
                    		boolean temDisponivelEmp5 = estDisponivelEmp5.doubleValue() > 0 
                    									&& saldo.doubleValue() <= estDisponivelEmp5.doubleValue();
                    		
                    		if (temDisponivelEmp5) {
                    			logDAO.create().set("DESCRICAO", ("TEM disponibilidade total empresa 5").toCharArray()).save();
                    			Split quebraEmp5 = new Split();
                    			quebraEmp5.codEmp = new BigDecimal("5");
                    			quebraEmp5.codProd = codProd;
                    			quebraEmp5.qtdNeg = saldo; 
                    			quebraPedido.add(quebraEmp5);
                    		} else {
                    			Split quebraEmp5 = new Split();
                    			quebraEmp5.codEmp = new BigDecimal("5");
                    			quebraEmp5.codProd = codProd;
                    			quebraEmp5.qtdNeg = estDisponivelEmp5; 
                    			quebraPedido.add(quebraEmp5);
                    			
                    			saldo = qtdNeg.subtract(estDisponivelEmp5);
                    			
                    			log = "NÃO TEM disponibilidade total empresa 5" + ", Saldo remanescente: " + saldo;
                    			logDAO.create().set("DESCRICAO", (log).toCharArray()).save();
                    			
                    			boolean temDisponivelEmp1 = saldo.doubleValue() > 0 
                    										&& estDisponivelEmp1.doubleValue() > 0 
                    										&& saldo.doubleValue() <= estDisponivelEmp1.doubleValue();
                    										
                    			if (temDisponivelEmp1) {
                    				logDAO.create().set("DESCRICAO", ("TEM disponibilidade total empresa 1").toCharArray()).save();
                    				Split quebraEmp1 = new Split();
                        			quebraEmp1.codEmp = new BigDecimal("1");
                        			quebraEmp1.codProd = codProd;
                        			quebraEmp1.qtdNeg = saldo; 
                        			quebraPedido.add(quebraEmp1);
                    			} else {
                    				Split quebraEmp1 = new Split();
                        			quebraEmp1.codEmp = new BigDecimal("1");
                        			quebraEmp1.codProd = codProd;
                        			quebraEmp1.qtdNeg = estDisponivelEmp1; 
                        			quebraPedido.add(quebraEmp1);
                        			
                        			saldo = saldo.subtract(estDisponivelEmp1);
                        			
                        			log = "NÃO TEM disponibilidade total empresa 1" + ", Saldo remanescente: " + saldo;
                        			logDAO.create().set("DESCRICAO", (log).toCharArray()).save();
                        			
                        			if (saldo.doubleValue() <= estDisponivelEmp6.doubleValue() && saldo.doubleValue() > 0) {
                        				logDAO.create().set("DESCRICAO", ("TEM disponibilidade total para transferencia da empresa 6 p/ 1").toCharArray()).save();
                        				logDAO.create().set("DESCRICAO", ("Transferindo quantidade necessária empresa 6 para 1: " + saldo).toCharArray()).save();

                        				itensTransferencia.add(new Transferencia(codProd, saldo));
                        				
                        				Split quebraEmp6 = new Split();
                            			quebraEmp6.codEmp = new BigDecimal("1");
                            			quebraEmp6.codProd = codProd;
                            			quebraEmp6.qtdNeg = saldo; 
                            			quebraPedido.add(quebraEmp6);
                            			
                        			} else {
                        				logDAO.create().set("DESCRICAO", ("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo").toCharArray()).save();
                        				TdbHelper.registraLogSplit("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo");
                        			}
                    			}
                    		}
                    	}
                    	
                    	BigDecimal nuNotaTransferencia = TdbHelper.transfereSaldo6x1(itensTransferencia);
                    	logDAO.create().set("DESCRICAO", ("Nro. Unico. Transferencia: " + nuNotaTransferencia).toCharArray()).save();
                    	
                    	for (Split pedido : quebraPedido) {
                    		TdbHelper.geraLancamentosSplit(pedidoVO.asBigDecimal("NUNOTA"), pedido);                    		
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
