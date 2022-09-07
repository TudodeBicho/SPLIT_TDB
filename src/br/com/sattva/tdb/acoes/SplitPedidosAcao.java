package br.com.sattva.tdb.acoes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sattva.tdb.job.Split;
import br.com.sattva.tdb.job.Transferencia;
import br.com.sattva.tdb.utils.CentralNotasUtils;
import br.com.sattva.tdb.utils.TdbHelper;

public class SplitPedidosAcao implements AcaoRotinaJava {
	public String statusProcessamento = "S";
	public String msgStatus = "";
	String log = "";
	Collection<Split> splitPedidos = new ArrayList<Split>();
	Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
	boolean geraTransferencia = false;
	final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
	final BigDecimal empresa1 = new BigDecimal("1"); // TDB
	final BigDecimal empresa6 = new BigDecimal("6"); // Loja

	@Override
	public void doAction(final ContextoAcao arg0) throws Exception {

		String nuNotaString = arg0.getParam("NUNOTA")+"";
		final BigDecimal nuNota = new BigDecimal(nuNotaString);
		
		SessionHandle hnd = null;
		
		try {
			
			hnd = JapeSession.open();
            hnd.execWithTX(new JapeSession.TXBlock() {
            	
            	JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
    	        JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
    	        JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
//    	        JapeWrapper produtoDAO = JapeFactory.dao("Produto");

	            public void doWithTx() throws Exception {
	
	            	String filtroPedidosAptos = "PENDENTE = 'S' "
                    				+ "AND CODVEND = 14 "
                    				+ "/*AND CODCIDDESTINO NOT IN (4798, 2475)*/ "
                    				+ "AND CODTIPOPER = 3102 "
                    				+ "AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) "
                    				+ "AND DTNEG >= '02/02/2022' AND CODEMP = 1";
	            	
	            	filtroPedidosAptos = "NUNOTA = " + nuNota;
	            	
                    Collection<DynamicVO> pedidosValidos = cabecalhoDAO.find(filtroPedidosAptos);
                    for (DynamicVO pedidoVO : pedidosValidos) {
                    	
                    	BigDecimal nuNotaOrig = pedidoVO.asBigDecimal("NUNOTA");
                    
                    	//Ordem de checagem de estoque: Extrema(5), TDB(1), Loja(6)
                    	Collection<DynamicVO> itensPedido = itemDAO.find("NUNOTA = ?", pedidoVO.asBigDecimal("NUNOTA"));
                    	
                    	TdbHelper tdbHelper = new TdbHelper();
                    	
                    	// Se o pedido/entrega for local, a prioridade deverá ser empresa 1. 
                    	// Caso contratio, deverá ser empresa 5
                    	
                    	
                    	if(pedidoVO.asBigDecimal("CODCIDDESTINO") != null && "4798-2475".indexOf(pedidoVO.asBigDecimal("CODCIDDESTINO").toString()) > -1) {
                    		log += "Regra de Prioridade: Empresa 1, Empresa 6 e Empresa 5\n";
                    		verificaDisponibilidade165(itensPedido, nuNotaOrig);
                    	} else {
                    		log += "Regra de Prioridade: Empresa 5, Empresa 1 e Empresa 6\n";
                    		verificaDisponibilidade516(itensPedido, nuNotaOrig);
                    	}
                    	
                    	/*
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
                            	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
                            	.set("DHINCLUSAO", TimeUtils.getNow())
                            	.save();
                    			return;
                    		}
                    		
                    		log = "QtdNeg: " + qtdNeg 
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
                    	*/
                    	
                    	if(geraTransferencia) {
                    		try {
                    			Map<String, BigDecimal> nroUnicoTransf = TdbHelper.transfereSaldo6x1(itensTransferencia);
                    			log += "\nNro.Unico.Transferencia Saida..: " + nroUnicoTransf.get("NUNOTATRANSFSAIDA");
                        		log += "\nNro.Unico.Transferencia Entrada: " + nroUnicoTransf.get("NUNOTATRANSFENTRADA");
							} catch (Exception e) {
								statusProcessamento = "E";
								msgStatus = e.toString();
								logDAO.create()
		                    	.set("DESCRICAO", log.toCharArray())
		                    	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
		                    	.set("DHINCLUSAO", TimeUtils.getNow())
		                    	.set("STATUSPROCESSAMENTO", statusProcessamento)
		                    	.set("MSGSTATUS", msgStatus)
		                    	.save();
								
								arg0.mostraErro(e.toString());
								return;
								
							}
                    	}
                    	
                    	Collection<Split> pedidosSplit = TdbHelper.agrupaSplitPorEmpresa(splitPedidos);
                    	Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa = TdbHelper.geraLancamentosSplit(pedidoVO, pedidosSplit);
                    	
                    	for (Split pedido : pedidosSplit) {
                    		for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {
                    			if (nuNotaEmp.getKey().intValue() == 1 && pedido.codEmp.intValue() == 1) {
                    				log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " +  pedido.toString();
                    			}
                    			if (nuNotaEmp.getKey().intValue() == 5 && pedido.codEmp.intValue() == 5) {
                    				log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " +  pedido.toString();
                    			}
                        	}
                    	}
                    	
                    	cabecalhoDAO.prepareToUpdate(pedidoVO)
                    	.set("CODTIPOPER", new BigDecimal("3131"))
                    	.set("DHTIPOPER", TdbHelper.getDhTipOper(new BigDecimal("3131")))
                    	.update();
                    	
                    	CentralNotasUtils.confirmarNota(pedidoVO.asBigDecimal("NUNOTA"));
                    	
                    	logDAO.create()
                    	.set("DESCRICAO", log.toCharArray())
                    	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
                    	.set("DHINCLUSAO", TimeUtils.getNow())
                    	.save();
                    }
	            }

	    });
			
//		} catch (Exception e) {
//			arg0.mostraErro(e.toString());
//			System.out.println("[Sattva]Split Error: " + e.toString());
		}finally {
			JapeSession.close(hnd);
		}
		arg0.setMensagemRetorno("Opa!");
	}
	
	private void verificaDisponibilidade516(Collection<DynamicVO> itensPedido, BigDecimal nuNotaOrig) throws Exception {
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
		
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
    			return;
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
	
	}
	
	private void verificaDisponibilidade156(Collection<DynamicVO> itensPedido, BigDecimal nuNotaOrig) throws Exception {
		
		JapeWrapper produtoDAO = JapeFactory.dao("Produto");
		JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
		
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
    			return;
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
		
		}
	
	
	private void verificaDisponibilidade165(Collection<DynamicVO> itensPedido, BigDecimal nuNotaOrig) throws Exception {
				
				JapeWrapper produtoDAO = JapeFactory.dao("Produto");
				JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
				
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
		    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);
		    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
		    		
		    		log += "QtdNeg: " + qtdNeg 
		    				+ ", estoqueDisponivelEmp1: " + estDisponivelEmp1
		    				+ ", estoqueDisponivelEmp6: " + estDisponivelEmp6
		    				+ ", estoqueDisponivelEmp5: " + estDisponivelEmp5;
		    		
		    		if (estDisponivelEmp1.doubleValue() == 0 && estDisponivelEmp6.doubleValue() == 0 && estDisponivelEmp5.doubleValue() == 0) {
		    			log += "Não possui estoque disponivel em nenhuma empresa para o produto " +codProd +"-"+nomeProduto+ ". Cancelando Split.";
		    			logDAO.create()
		            	.set("DESCRICAO", log.toCharArray())
		            	.set("NUNOTAORIG", nuNotaOrig)
		            	.set("DHINCLUSAO", TimeUtils.getNow())
		            	.save();
		    			return;
		    		}
		    		
		    		boolean temDisponivelEmp1 = 
		    				estDisponivelEmp1.doubleValue() > 0 
		    				&& saldo.doubleValue() <= estDisponivelEmp1.doubleValue()
		    				&& saldo.doubleValue() > 0;
		    		
		    		if (temDisponivelEmp1) {
		    			log += "\nTEM disponibilidade total empresa 1";
		    			splitPedidos.add(new Split(empresa1, codProd, saldo)); 
		    			
		    		} else {
		    			splitPedidos.add(new Split(empresa1, codProd, estDisponivelEmp1));
		    			saldo = qtdNeg.subtract(estDisponivelEmp1);
		    			log += "\nNÃO TEM disponibilidade total empresa 1, consumindo: " + estDisponivelEmp1 + ", Saldo remanescente: " + saldo;
		    			
		    			boolean temDisponivelEmp6 = 
		    					saldo.doubleValue() > 0 
		    					&& estDisponivelEmp6.doubleValue() > 0 
		    					&& saldo.doubleValue() <= estDisponivelEmp6.doubleValue();
		    										
		    			if (temDisponivelEmp6) {
		    				geraTransferencia = true;
		    				log += "\nTEM disponibilidade do saldo para transferencia da empresa 6 p/ 1";
		    				log += "\nTransferindo: quantidade" + saldo + " necessária da empresa 6 para 1";
		    				itensTransferencia.add(new Transferencia(codProd, saldo));
		        			splitPedidos.add(new Split(empresa1, codProd, saldo));
		        			
		    			} else {
		    				itensTransferencia.add(new Transferencia(codProd, estDisponivelEmp6));
		        			splitPedidos.add(new Split(empresa1, codProd, estDisponivelEmp6));
		        			saldo = saldo.subtract(estDisponivelEmp6);
		        			log += "\nNÃO TEM disponibilidade total empresa 6, consumindo: " + estDisponivelEmp6 + ", Saldo remanescente: " + saldo;
		        			
		        			if (saldo.doubleValue() <= estDisponivelEmp5.doubleValue() && saldo.doubleValue() > 0) {
		        				log += "\nTEM disponibilidade do saldo para consumo na empresa 5: " + saldo;
		            			splitPedidos.add(new Split(empresa5, codProd, saldo));
		        			} else {
		        				log += "\nNão é possivel fazer o split pois não tem estoque suficiente em todo o grupo";
		        				TdbHelper.registraLogSplit("Não é possivel fazer o split pois não tem estoque suficiente em todo o grupo");
		        			}
		    			}
		    		}
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
